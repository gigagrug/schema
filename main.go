package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"

	"charm.land/bubbles/v2/help"
	"charm.land/bubbles/v2/key"
	"charm.land/bubbles/v2/list"
	"charm.land/bubbles/v2/table"
	"charm.land/bubbles/v2/textarea"
	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/joho/godotenv"
	"github.com/tliron/commonlog"
	_ "github.com/tliron/commonlog/simple"
	protocol "github.com/tliron/glsp/protocol_3_16"
	"github.com/tliron/glsp/server"
	_ "github.com/tursodatabase/libsql-client-go/libsql"
	_ "modernc.org/sqlite"
	_ "turso.tech/database/tursogo"
)

var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if len(os.Args) < 2 {
		fmt.Print(`
 ____       _                          
/ ___|  ___| |__   ___ ________   ____ 
\___ \ / __| '_ \ / _ \  _   _ \ / _  |
 ___) | (__| | | |  __/ | | | | | (_| |
|____/ \___|_| |_|\___|_| |_| |_|\__,_|
`)
		os.Exit(0)
	}

	switch os.Args[1] {
	case "help", "h":
		printHelp()
	case "version", "v":
		checkVersion(ctx)
	case "init", "i":
		runInit(os.Args[2:])
	case "create":
		runCreate(ctx, os.Args[2:])
	case "migrate":
		runMigrate(ctx, os.Args[2:])
	case "studio":
		runStudio(os.Args[2:])
	case "rollback":
		runRollback(ctx, os.Args[2:])
	case "remove", "rm":
		runRemove(ctx, os.Args[2:])
	case "pull":
		runPull(ctx, os.Args[2:])
	case "sql":
		runSQL(ctx, os.Args[2:])
	case "config":
		runConfig(os.Args[2:])
	case "lsp":
		runLSP(os.Args[2:])
	default:
		fmt.Printf("Unknown command: %s\nExpected Subcommands: studio, migrate, create, rollback, init, pull, sql, lsp, help, version, config\n", os.Args[1])
		os.Exit(0)
	}
}

func printHelp() {
	fmt.Fprintf(os.Stderr, "Path: %s\n", os.Args[0])
	fmt.Println("Subcommands:")
	fmt.Println("  init         Initialize database schema.db and .env")
	fmt.Println("  config       Update database configuration (url, db type)")
	fmt.Println("  studio       Open the TUI database studio")
	fmt.Println("  migrate      Run pending migrations")
	fmt.Println("  create       Create a new migration file")
	fmt.Println("  rollback     Rollback the last migration")
	fmt.Println("  remove       Remove a migration file")
	fmt.Println("  pull         Update schema.db file from database")
	fmt.Println("  sql          Run a raw SQL query or file")
	fmt.Println("  lsp          Start the language server")
	fmt.Println("  version      Check version")
	fmt.Println()
	fmt.Println("Flags:")
	fmt.Println("  -db          db type (default \"sqlite\")")
	fmt.Println("  -url         db url (default \"./schema/dev.db\")")
	fmt.Println("  -rdir        Root directory (default \"schema\")")
	fmt.Println("  -dir         Migrations directory (default \"migrations\")")
}

func checkVersion(ctx context.Context) {
	fmt.Println("Version:", version)

	url := "https://api.github.com/repos/gigagrug/schema/releases/latest"
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		log.Fatalf("Error creating request: %v\n", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatalf("Error fetching release data from GitHub: %v\n", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("Error reading GitHub response body: %v\n", err)
	}

	var result map[string]any
	err = json.Unmarshal(body, &result)
	if err != nil {
		log.Fatalf("Error unmarshalling JSON: %v\n", err)
	}

	latestVersion, ok := result["tag_name"].(string)
	if !ok {
		log.Fatalf("Could not find 'tag_name' or it was not a string in the GitHub release data\n")
	}

	if version != latestVersion {
		fmt.Printf("Outdated! Latest version: %s\n", latestVersion)
		fmt.Printf("curl -sSfL https://raw.githubusercontent.com/gigagrug/schema/main/install.sh | sh -s\n")
	} else {
		fmt.Println("Using latest version")
	}
}

func runInit(args []string) {
	cmd := flag.NewFlagSet("init", flag.ExitOnError)
	db := cmd.String("db", "sqlite", "database type")
	url := cmd.String("url", filepath.Join("schema", "dev.db"), "database url")
	rdir := cmd.String("rdir", "schema", "root directory")
	cmd.Parse(args)

	schemaPath := filepath.Join(*rdir, "db.schema")

	if !isFlagPassed(cmd, "url") && *db == "sqlite" {
		*url = filepath.Join(*rdir, "dev.db")
	}

	if _, err := os.Stat(schemaPath); os.IsNotExist(err) {
		err := os.Mkdir(filepath.Join(*rdir), 0700)
		if err != nil {
			log.Fatalf("Error creating schema/migrations directory: %v\n", err)
		}
		schemaFile, err := os.Create(schemaPath)
		if err != nil {
			log.Fatalf("Error creating file: %v\n", err)
		}
		defer schemaFile.Close()

		fileContent := fmt.Sprintf("db = \"%s\"\nurl = env(\"%s_DB_URL\")", *db, strings.ToUpper(*rdir))
		_, err = schemaFile.WriteString(fileContent)
		if err != nil {
			log.Fatalf("Error writing to file: %v\n", err)
		}
	}

	envPath := ".env"
	safeUrl := filepath.ToSlash(*url)

	if _, err := os.Stat(envPath); os.IsNotExist(err) {
		envFile, err := os.Create(envPath)
		if err != nil {
			log.Fatalf("Error creating .env file: %v\n", err)
		}
		defer envFile.Close()

		schemaContent := fmt.Sprintf(`%s_DB_URL="%s"`, strings.ToUpper(*rdir), safeUrl)
		_, err = envFile.WriteString(schemaContent)
		if err != nil {
			log.Fatalf("Error writing to .env file: %v\n", err)
		}
	} else {
		envFile, err := os.OpenFile(envPath, os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			log.Fatalf("Error opening .env file: %v\n", err)
		}
		defer envFile.Close()

		schemaContent := fmt.Sprintf("\n%s_DB_URL=\"%s\"", strings.ToUpper(*rdir), safeUrl)
		_, err = envFile.WriteString(schemaContent)
		if err != nil {
			log.Fatalf("Error appending to .env file: %v\n", err)
		}
	}
	fmt.Println("Schema successfully initialized")
}

func runCreate(ctx context.Context, args []string) {
	cmd := flag.NewFlagSet("create", flag.ExitOnError)
	db := cmd.String("db", "", "update database type")
	url := cmd.String("url", "", "update connection url")
	dir := cmd.String("dir", "migrations", "directory path")
	rdir := cmd.String("rdir", "schema", "root directory")
	cmd.Parse(args)

	var createName string
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		createName = args[0]
		cmd.Parse(args[1:])
	} else {
		cmd.Parse(args)
		if len(cmd.Args()) > 0 {
			createName = cmd.Args()[0]
		}
	}

	if createName == "" {
		log.Fatal("File name required.")
	}

	schemaPath := filepath.Join(*rdir, "db.schema")
	conn, dbtype, err := Conn2DB(schemaPath, *db, *url)
	if err != nil {
		log.Fatalf("Error connecting: %v", err)
	}
	defer conn.Close()
	dialect := GetDialect(dbtype)

	if *dir == "migrations" {
		CheckTableExists(ctx, conn, dbtype, *rdir)
	}

	dirPath := filepath.Join(*rdir, *dir)
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		err := os.MkdirAll(dirPath, 0700)
		if err != nil {
			log.Fatalf("Error creating %s: %v\n", dirPath, err)
		}
	}

	entries, err := os.ReadDir(dirPath)
	if err != nil {
		log.Fatalf("Failed to read directory '%s': %v", dirPath, err)
	}

	maxPrefix := -1
	for _, entry := range entries {
		if !entry.IsDir() {
			parts := strings.SplitN(entry.Name(), "_", 2)
			if len(parts) > 0 {
				prefix, err := strconv.Atoi(parts[0])
				if err == nil && prefix > maxPrefix {
					maxPrefix = prefix
				}
			}
		}
	}
	fileCount := maxPrefix + 1

	fileName := fmt.Sprintf("%d_%s.sql", fileCount, createName)
	schemaFile, err := os.Create(filepath.Join(dirPath, fileName))
	if err != nil {
		log.Fatalf("Error creating file: %v\n", err)
	}
	defer schemaFile.Close()

	if *dir == "migrations" {
		template := "\n\n-- schema rollback\n\n"
		_, err = schemaFile.WriteString(template)
		if err != nil {
			log.Fatalf("Error writing template to file: %v", err)
		}

		_, err = conn.ExecContext(ctx, dialect.Insert, fileName, false)
		if err != nil {
			log.Fatalf("Error executing SQL: %v\n", err)
		}
	}
	fmt.Printf("Schema successfully created sql file %s\n", fileName)
}

func runConfig(args []string) {
	cmd := flag.NewFlagSet("config", flag.ExitOnError)
	db := cmd.String("db", "", "update database type")
	url := cmd.String("url", "", "update connection url")
	rdir := cmd.String("rdir", "schema", "root directory")
	cmd.Parse(args)

	schemaPath := filepath.Join(*rdir, "db.schema")
	envPath := ".env"
	envVarName := strings.ToUpper(*rdir) + "_DB_URL"

	if *url != "" {
		file, err := os.OpenFile(envPath, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			log.Fatalf("Error opening .env: %v", err)
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		var lines []string
		var found bool

		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, envVarName+"=") {
				lines = append(lines, fmt.Sprintf(`%s="%s"`, envVarName, *url))
				found = true
			} else {
				lines = append(lines, line)
			}
		}
		if !found {
			lines = append(lines, fmt.Sprintf(`%s="%s"`, envVarName, *url))
		}

		file.Truncate(0)
		file.Seek(0, 0)
		writer := bufio.NewWriter(file)
		for _, line := range lines {
			writer.WriteString(line + "\n")
		}
		writer.Flush()
		fmt.Printf("Updated .env: %s\n", *url)
	}

	if *db != "" {
		file, err := os.OpenFile(schemaPath, os.O_RDWR, 0600)
		if err != nil {
			log.Fatalf("Error opening schema file: %v", err)
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		var lines []string
		var found bool

		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "db =") {
				lines = append(lines, fmt.Sprintf(`db = "%s"`, *db))
				found = true
			} else {
				lines = append(lines, line)
			}
		}
		if !found {
			lines = append(lines, fmt.Sprintf(`db = "%s"`, *db))
		}

		file.Truncate(0)
		file.Seek(0, 0)
		writer := bufio.NewWriter(file)
		for _, line := range lines {
			writer.WriteString(line + "\n")
		}
		writer.Flush()
		fmt.Printf("Updated schema db type: %s\n", *db)
	}

	if *url == "" && *db == "" {
		fmt.Println("No flags provided. Use -url or -db to update configuration.")
	}
}

func runStudio(args []string) {
	cmd := flag.NewFlagSet("studio", flag.ExitOnError)
	db := cmd.String("db", "", "database type")
	url := cmd.String("url", "", "database url")
	rdir := cmd.String("rdir", "schema", "root directory")
	cmd.Parse(args)

	if *url != "" {
		os.Setenv(strings.ToUpper(*rdir)+"_DB_URL", *url)
	}
	schemaPath := filepath.Join(*rdir, "db.schema")

	conn, dbtype, err := Conn2DB(schemaPath, *db, *url)
	if err != nil {
		log.Fatalf("Error connecting to database: %v", err)
	}
	defer conn.Close()

	p := tea.NewProgram(initialModel(conn, dbtype))
	if _, err := p.Run(); err != nil {
		log.Fatalf("Error running studio: %v", err)
	}
}

func runMigrate(ctx context.Context, args []string) {
	cmd := flag.NewFlagSet("migrate", flag.ExitOnError)
	db := cmd.String("db", "", "database type")
	url := cmd.String("url", "", "connection url")
	rdir := cmd.String("rdir", "schema", "root directory")

	var targetFile string
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		targetFile = args[0]
		cmd.Parse(args[1:])
	} else {
		cmd.Parse(args)
		if len(cmd.Args()) > 0 {
			targetFile = cmd.Args()[0]
		}
	}

	schemaPath := filepath.Join(*rdir, "db.schema")
	conn, dbtype, err := Conn2DB(schemaPath, *db, *url)
	if err != nil {
		log.Fatalf("Error connecting: %v", err)
	}
	defer conn.Close()

	CheckTableExists(ctx, conn, dbtype, *rdir)
	dialect := GetDialect(dbtype)

	migrationsDir := filepath.Join(*rdir, "migrations")
	localMigrationFiles, err := os.ReadDir(migrationsDir)
	if err != nil {
		log.Fatalf("Error reading migrations directory '%s': %v\n", migrationsDir, err)
	}

	dbMigrationFiles := make(map[string]bool)

	rows, err := conn.QueryContext(ctx, "SELECT file FROM _schema_migrations")
	if err != nil {
		log.Fatalf("Error querying _schema_migrations table: %v\n", err)
	}
	defer rows.Close()
	for rows.Next() {
		var filename string
		if err := rows.Scan(&filename); err != nil {
			log.Fatalf("Error scanning migration file from DB: %v\n", err)
		}
		dbMigrationFiles[filename] = true
	}

	for _, entry := range localMigrationFiles {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") {
			if _, exists := dbMigrationFiles[entry.Name()]; !exists {
				_, err = conn.ExecContext(ctx, dialect.Insert, entry.Name(), false)
				if err != nil {
					fmt.Printf("Warning: Could not add migration file '%s' to _schema_migrations table: %v\n", entry.Name(), err)
				} else {
					fmt.Printf("Added new migration file '%s' to _schema_migrations table.\n", entry.Name())
				}
			}
		}
	}

	if targetFile != "" {
		migrationFileName := targetFile
		if !strings.HasSuffix(migrationFileName, ".sql") {
			migrationFileName += ".sql"
		}

		fileP := filepath.Join(*rdir, "migrations", migrationFileName)
		sqlFile, err := os.ReadFile(fileP)
		if err != nil {
			log.Fatalf("Error reading SQL file: %v\n", err)
		}

		sqlContent := string(sqlFile)
		migrationSQL := strings.Split(sqlContent, "-- schema rollback")[0]

		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			log.Fatalf("Error starting transaction: %v", err)
		}
		defer tx.Rollback()

		_, err = tx.ExecContext(ctx, migrationSQL)
		if err != nil {
			log.Fatalf("Error executing SQL (rolled back): %v\n", err)
		}

		_, err = tx.ExecContext(ctx, dialect.Update, true, migrationFileName)
		if err != nil {
			log.Fatalf("Error updating status (rolled back): %v\n", err)
		}

		if err := tx.Commit(); err != nil {
			log.Fatalf("Error committing transaction: %v\n", err)
		}

		err = PullDBSchema(ctx, conn, dbtype, schemaPath)
		if err != nil {
			log.Fatalf("Error pulling DB schema after migration: %v\n", err)
		}
		fmt.Printf("Schema successfully migrated %s\n", migrationFileName)

	} else {
		rows, err := conn.QueryContext(ctx, `SELECT file FROM _schema_migrations WHERE migrated = false ORDER BY id ASC`)
		if err != nil {
			log.Fatalf("Error executing SQL query for pending migrations: %v\n", err)
		}

		type Files struct{ Name string }
		files := []Files{}
		for rows.Next() {
			var fName string
			err = rows.Scan(&fName)
			if err != nil {
				log.Fatalf("Error scanning row for pending migration file: %v\n", err)
			}
			files = append(files, Files{Name: fName})
		}
		rows.Close()

		if len(files) == 0 {
			fmt.Println("No pending migrations found.")
			return
		}

		for _, entry := range files {
			err := func() error {
				fileP := filepath.Join(*rdir, "migrations", entry.Name)
				sqlFile, err := os.ReadFile(fileP)
				if err != nil {
					return fmt.Errorf("reading file: %w", err)
				}

				sqlContent := string(sqlFile)
				migrationSQL := strings.Split(sqlContent, "-- schema rollback")[0]

				tx, err := conn.BeginTx(ctx, nil)
				if err != nil {
					return fmt.Errorf("starting transaction: %w", err)
				}
				defer tx.Rollback()

				if _, err := tx.ExecContext(ctx, migrationSQL); err != nil {
					return fmt.Errorf("executing migration SQL: %w", err)
				}

				if _, err := tx.ExecContext(ctx, dialect.Update, true, entry.Name); err != nil {
					return fmt.Errorf("updating migration status: %w", err)
				}

				return tx.Commit()
			}()

			if err != nil {
				log.Fatalf("Migration failed for %s: %v", entry.Name, err)
			}

			err = PullDBSchema(ctx, conn, dbtype, schemaPath)
			if err != nil {
				log.Fatalf("Error pulling DB schema after migration %s: %v\n", entry.Name, err)
			}
			fmt.Printf("Schema successfully migrated %s\n", entry.Name)
		}
	}
}

func runRollback(ctx context.Context, args []string) {
	cmd := flag.NewFlagSet("rollback", flag.ExitOnError)
	db := cmd.String("db", "", "database type")
	url := cmd.String("url", "", "connection url")
	dir := cmd.String("dir", "migrations", "migrations directory")
	rdir := cmd.String("rdir", "schema", "root directory")

	var targetFile string
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		targetFile = args[0]
		cmd.Parse(args[1:])
	} else {
		cmd.Parse(args)
		if len(cmd.Args()) > 0 {
			targetFile = cmd.Args()[0]
		}
	}

	schemaPath := filepath.Join(*rdir, "db.schema")
	conn, dbtype, err := Conn2DB(schemaPath, *db, *url)
	if err != nil {
		log.Fatalf("Error connecting: %v", err)
	}
	defer conn.Close()
	dialect := GetDialect(dbtype)

	var migrationToRollback string
	var migrationFileName string

	if targetFile == "" {
		query := `SELECT file FROM _schema_migrations WHERE migrated = true ORDER BY id DESC LIMIT 1`
		err := conn.QueryRowContext(ctx, query).Scan(&migrationFileName)
		if err != nil {
			if err == sql.ErrNoRows {
				log.Println("No migrations to rollback.")
				return
			}
			log.Fatalf("Error finding last migration to rollback: %v\n", err)
		}
		migrationToRollback = strings.TrimSuffix(migrationFileName, ".sql")
	} else {
		migrationToRollback = targetFile
		migrationToRollback = strings.TrimSuffix(migrationToRollback, ".sql")
		migrationFileName = migrationToRollback + ".sql"
	}

	fileP := filepath.Join(*rdir, *dir, migrationToRollback+".sql")
	sqlFile, err := os.ReadFile(fileP)
	if err != nil {
		log.Fatalf("Error reading SQL file for rollback: %v\n", err)
	}

	sqlContent := string(sqlFile)
	parts := strings.Split(sqlContent, "-- schema rollback")

	if len(parts) < 2 || strings.TrimSpace(parts[1]) == "" {
		log.Fatalf("Error: No rollback script found in %s.sql", migrationToRollback)
	}
	rollbackSQL := parts[1]

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		log.Fatalf("Error starting transaction: %v", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, rollbackSQL); err != nil {
		log.Fatalf("Error executing rollback SQL for %s.sql: %v\n", migrationToRollback, err)
	}

	if *dir == "migrations" {
		if _, err := tx.ExecContext(ctx, dialect.Update, false, migrationFileName); err != nil {
			log.Fatalf("Error updating migration status after rollback for %s: %v\n", migrationFileName, err)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Fatalf("Error committing rollback transaction: %v\n", err)
	}

	err = PullDBSchema(ctx, conn, dbtype, schemaPath)
	if err != nil {
		log.Fatalf("Error pulling DB schema after rollback: %v\n", err)
	}

	fmt.Printf("Successfully rolled back migration %s\n", migrationFileName)
}

func runPull(ctx context.Context, args []string) {
	cmd := flag.NewFlagSet("pull", flag.ExitOnError)
	db := cmd.String("db", "", "database type")
	url := cmd.String("url", "", "connection url")
	rdir := cmd.String("rdir", "schema", "root directory")
	cmd.Parse(args)

	schemaPath := filepath.Join(*rdir, "db.schema")
	conn, dbtype, err := Conn2DB(schemaPath, *db, *url)
	if err != nil {
		log.Fatalf("Error connecting: %v", err)
	}
	defer conn.Close()

	err = PullDBSchema(ctx, conn, dbtype, schemaPath)
	if err != nil {
		log.Fatalf("Err pulling db schema: %v\n", err)
	}
}

func runRemove(ctx context.Context, args []string) {
	cmd := flag.NewFlagSet("remove", flag.ExitOnError)
	db := cmd.String("db", "", "database type")
	url := cmd.String("url", "", "connection url")
	rdir := cmd.String("rdir", "schema", "root directory")
	cmd.Parse(args)

	var name string
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		name = args[0]
		cmd.Parse(args[1:])
	} else {
		cmd.Parse(args)
		name = cmd.Arg(0)
	}
	if name == "" {
		log.Fatal("Usage: remove <migration_name>")
	}

	schemaPath := filepath.Join(*rdir, "db.schema")
	conn, dbtype, err := Conn2DB(schemaPath, *db, *url)
	if err != nil {
		log.Fatalf("Error connecting: %v", err)
	}
	defer conn.Close()
	dialect := GetDialect(dbtype)

	migrationFileName := name
	if !strings.HasSuffix(migrationFileName, ".sql") {
		migrationFileName += ".sql"
	}

	var migrated bool
	err = conn.QueryRowContext(ctx, dialect.SelectStatus, migrationFileName).Scan(&migrated)
	if err != nil && err != sql.ErrNoRows {
		log.Fatalf("Error checking migration status for %s: %v\n", migrationFileName, err)
	}

	if err == nil && migrated {
		log.Fatalf("Cannot remove migration file '%s' because it has already been migrated.", migrationFileName)
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		log.Fatalf("Error starting transaction: %v", err)
	}
	defer tx.Rollback()

	if err != sql.ErrNoRows {
		_, delErr := tx.ExecContext(ctx, dialect.Delete, migrationFileName)
		if delErr != nil {
			log.Fatalf("Failed to delete migration record for '%s' from database: %v\n", migrationFileName, delErr)
		}
	}

	filePath := filepath.Join(*rdir, "migrations", migrationFileName)
	removeErr := os.Remove(filePath)
	if removeErr != nil {
		if os.IsNotExist(removeErr) {
			fmt.Printf("Migration file '%s' not found on filesystem, but its database record was removed.\n", migrationFileName)
		} else {
			log.Fatalf("Error removing migration file '%s' from filesystem (DB changes rolled back): %v\n", filePath, removeErr)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Fatalf("Error committing transaction: %v", err)
	}

	if removeErr == nil {
		fmt.Printf("Successfully removed migration file '%s' and its database record.\n", migrationFileName)
	}
}

func runSQL(ctx context.Context, args []string) {
	cmd := flag.NewFlagSet("sql", flag.ExitOnError)
	db := cmd.String("db", "", "database type")
	url := cmd.String("url", "", "database url")
	rdir := cmd.String("rdir", "schema", "root directory")
	dir := cmd.String("dir", "migrations", "directory")
	cmd.Parse(args)

	var query string

	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		query = args[0]
		cmd.Parse(args[1:])
	} else {
		cmd.Parse(args)
		query = cmd.Arg(0)
	}

	if query == "" {
		log.Fatal("Usage: sql \"SELECT ...\" or sql filename.sql")
	}

	schemaPath := filepath.Join(*rdir, "db.schema")
	conn, _, err := Conn2DB(schemaPath, *db, *url)
	if err != nil {
		log.Fatalf("Error connecting: %v", err)
	}
	defer conn.Close()

	if strings.HasSuffix(strings.TrimSpace(query), ".sql") {
		fileP := filepath.Join(*rdir, *dir, query)
		sqlFile, err := os.ReadFile(fileP)
		if err != nil {
			log.Fatalf("Error reading SQL file: %v\n", err)
		}
		rows, err := conn.QueryContext(ctx, string(sqlFile))
		if err != nil {
			log.Fatalf("Error executing SQL query: %v\n", err)
		}
		defer rows.Close()

		columns, err := rows.Columns()
		if err != nil {
			log.Fatalf("Error getting columns: %v\n", err)
		}

		var data [][]string
		values := make([]any, len(columns))
		scanArgs := make([]any, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}
		for rows.Next() {
			rows.Scan(scanArgs...)
			row := make([]string, len(columns))
			for i, col := range values {
				if col == nil {
					row[i] = "NULL"
				} else {
					row[i] = fmt.Sprintf("%v", col)
				}
			}
			data = append(data, row)
		}
		fmt.Println(printTable(columns, data))
		fmt.Printf("SQL file executed successfully: %s\n", query)
		return
	}

	if strings.HasPrefix(strings.TrimSpace(strings.ToUpper(query)), "SELECT") {
		rows, err := conn.QueryContext(ctx, query)
		if err != nil {
			log.Fatalf("Error executing SQL query: %v\n", err)
		}
		defer rows.Close()
		columns, err := rows.Columns()
		if err != nil {
			log.Fatalf("Error getting columns: %v\n", err)
		}
		var data [][]string
		values := make([]any, len(columns))
		scanArgs := make([]any, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}
		for rows.Next() {
			rows.Scan(scanArgs...)
			row := make([]string, len(columns))
			for i, col := range values {
				if col == nil {
					row[i] = "NULL"
				} else {
					row[i] = fmt.Sprintf("%v", col)
				}
			}
			data = append(data, row)
		}
		fmt.Println(printTable(columns, data))

	} else {
		result, err := conn.ExecContext(ctx, query)
		if err != nil {
			log.Fatalf("Error executing SQL command: %v\n", err)
		}
		rowsAffected, _ := result.RowsAffected()
		fmt.Printf("SQL command executed successfully. Rows affected: %d\n", rowsAffected)
	}
}

func runLSP(args []string) {
	cmd := flag.NewFlagSet("lsp", flag.ExitOnError)
	db := cmd.String("db", "", "database type")
	url := cmd.String("url", "", "database url")
	rdir := cmd.String("rdir", "schema", "root directory")
	cmd.Parse(args)

	schemaPath := filepath.Join(*rdir, "db.schema")
	conn, dbtype, err := Conn2DB(schemaPath, *db, *url)
	if err != nil {
		log.Fatalf("Error connecting to database: %v", err)
	}
	defer conn.Close()

	log.Println("Starting LSP server...")
	commonlog.Configure(1, nil)
	lspActiveDbType = dbtype

	log.Println("LSP: Caching database schema...")
	tables, err := getSQLTables(conn, dbtype)
	if err != nil {
		log.Fatalf("LSP failed to get tables: %v", err)
	}

	for _, table := range tables {
		columns, err := getSQLColumns(conn, dbtype, table)
		if err != nil {
			log.Printf("LSP warning: could not get columns for table %s: %v", table, err)
			continue
		}
		dbSchemaCache[table] = columns
	}
	log.Printf("LSP: Cached %d tables.", len(dbSchemaCache))

	handler = protocol.Handler{
		Initialize:             initialize,
		Initialized:            initialized,
		Shutdown:               shutdown,
		SetTrace:               setTrace,
		TextDocumentCompletion: textDocumentCompletion,
		TextDocumentDidOpen:    textDocumentDidOpen,
		TextDocumentDidChange:  textDocumentDidChange,
		TextDocumentDidSave:    textDocumentDidSave,
		TextDocumentFormatting: textDocumentFormatting,
	}

	server := server.NewServer(&handler, lspName, false)
	server.RunStdio()
}

func CheckTableExists(ctx context.Context, conn *sql.DB, dbtype string, rdir string) {
	dialect := GetDialect(dbtype)
	if dialect.Type == "" {
		log.Fatalf("Unsupported database type for table existence check: %s", dbtype)
	}

	var name string
	err := conn.QueryRowContext(ctx, dialect.TableExists).Scan(&name)

	if err == sql.ErrNoRows {
		migrationsDir := filepath.Join(rdir, "migrations")
		if _, dirErr := os.Stat(migrationsDir); os.IsNotExist(dirErr) {
			err = os.MkdirAll(migrationsDir, 0700)
			if err != nil {
				log.Fatalf("Error creating migrations directory: %v\n", err)
			}
		}

		initFilePath := filepath.Join(rdir, "migrations", "0_init.sql")
		if _, fileErr := os.Stat(initFilePath); os.IsNotExist(fileErr) {
			file, err := os.Create(initFilePath)
			if err != nil {
				log.Fatalf("Error creating 0_init.sql file: %v\n", err)
			}
			defer file.Close()

			if dbtype == "sqlite" {
				_, _ = file.WriteString("PRAGMA journal_mode=WAL;\n\n")
			}
			_, err = file.WriteString(dialect.CreateInit)
			if err != nil {
				log.Fatalf("Error writing to 0_init.sql file: %v\n", err)
			}
		}

		sqlFile, err := os.ReadFile(initFilePath)
		if err != nil {
			log.Fatalf("Error reading SQL file: %v\n", err)
		}

		_, err = conn.ExecContext(ctx, string(sqlFile))
		if err != nil {
			log.Fatalf("Error executing SQL to create _schema_migrations table: %v\n", err)
		}

		_, err = conn.ExecContext(ctx, dialect.Insert, "0_init.sql", true)
		if err != nil {
			log.Fatalf("Error executing SQL to insert 0_init.sql record: %v\n", err)
		}

		err = PullDBSchema(ctx, conn, dbtype, filepath.Join(rdir, "db.schema"))
		if err != nil {
			log.Fatalf("Migrate2: Err pulling schema %v\n", err)
		}

		fmt.Println("Schema DB successfully initialized")
		return
	} else if err != nil {
		log.Fatalf("Error querying table existence: %v\n", err)
	}
}

func isFlagPassed(fs *flag.FlagSet, name string) bool {
	found := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

func Conn2DB(schemaFilePath, overrideDB, overrideURL string) (*sql.DB, string, error) {
	if overrideDB != "" && overrideURL != "" {
		var driverName string
		switch overrideDB {
		case "sqlite":
			driverName = "sqlite"
		case "postgres":
			driverName = "pgx"
		case "mysql", "mariadb":
			driverName = "mysql"
		case "libsql":
			driverName = "libsql"
		case "turso":
			driverName = "turso"
		default:
			return nil, "", fmt.Errorf("unsupported database type '%s'", overrideDB)
		}
		conn, err := sql.Open(driverName, overrideURL)
		if err != nil {
			return nil, "", fmt.Errorf("failed to open DB connection: %v", err)
		}
		return conn, overrideDB, nil
	}

	err := godotenv.Load()
	if err != nil {
		return nil, "", fmt.Errorf("error loading .env file: %w", err)
	}

	file, err := os.Open(schemaFilePath)
	if err != nil {
		return nil, "", fmt.Errorf("failed to open schema file '%s': %w", schemaFilePath, err)
	}
	defer file.Close()

	var dbType, dbURL string
	foundDbType := false
	lineNumber := 0
	dbTypePrefix := "db ="
	dbURLPrefix := "url ="
	envRegex := regexp.MustCompile(`env\("([^"]+)"\)`)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lineNumber++
		line := strings.TrimSpace(scanner.Text())

		if strings.HasPrefix(line, dbTypePrefix) {
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				dbType = strings.Trim(strings.TrimSpace(parts[1]), "\"'")
				foundDbType = true
			} else {
				fmt.Printf("Warning: Invalid '%s' format in schema '%s' on line %d: %s\n", dbTypePrefix, schemaFilePath, lineNumber, line)
			}
		}

		if strings.HasPrefix(line, dbURLPrefix) {
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				value := strings.Trim(strings.TrimSpace(parts[1]), "\"'")
				matches := envRegex.FindStringSubmatch(value)
				if len(matches) == 2 {
					envVarName := matches[1]
					dbURL = os.Getenv(envVarName)
					if dbURL == "" {
						fmt.Printf("Warning: Environment variable '%s' not found in .env (referenced in '%s' on line %d)\n", envVarName, schemaFilePath, lineNumber)
					}
				} else {
					dbURL = value
				}
			} else {
				fmt.Printf("Warning: Invalid '%s' format in schema '%s' on line %d: %s\n", dbURLPrefix, schemaFilePath, lineNumber, line)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, "", fmt.Errorf("error reading schema file '%s': %w", schemaFilePath, err)
	}

	if !foundDbType {
		return nil, "", fmt.Errorf("could not find a line starting with '%s' in schema file '%s'", dbTypePrefix, schemaFilePath)
	}

	if dbURL == "" {
		return nil, "", fmt.Errorf("could not determine database URL in schema file '%s'", schemaFilePath)
	}

	var driverName string
	switch dbType {
	case "sqlite":
		driverName = "sqlite"
	case "postgres":
		driverName = "pgx"
	case "mysql", "mariadb":
		driverName = "mysql"
	case "libsql":
		driverName = "libsql"
	case "turso":
		driverName = "turso"
	default:
		return nil, "", fmt.Errorf("unsupported database type '%s' in schema '%s'", dbType, schemaFilePath)
	}
	conn, err := sql.Open(driverName, dbURL)
	if err != nil {
		return nil, "", fmt.Errorf("failed to open DB connection: %v", err)
	}
	return conn, dbType, nil
}

func splitOnTopLevelCommas(s string) []string {
	var parts []string
	parenLevel := 0
	lastSplit := 0
	for i, char := range s {
		switch char {
		case '(':
			parenLevel++
		case ')':
			parenLevel--
		case ',':
			if parenLevel == 0 {
				parts = append(parts, s[lastSplit:i])
				lastSplit = i + 1
			}
		}
	}
	parts = append(parts, s[lastSplit:])
	return parts
}

func PullDBSchema(ctx context.Context, conn *sql.DB, dbtype, schemaFilePath string) error {
	var schema string
	type ForeignKey struct {
		ConstraintName    string
		TableName         string
		ColumnName        string
		ForeignTableName  string
		ForeignColumnName string
		OnDelete          string
		OnUpdate          string
	}
	switch dbtype {
	case "sqlite", "libsql", "turso":
		rows, err := conn.QueryContext(ctx, "SELECT sql FROM sqlite_master WHERE sql NOT NULL AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_schema_migrations'")
		if err != nil {
			return fmt.Errorf("error querying sqlite: %w", err)
		}
		defer rows.Close()

		var tableSchemas []string
		for rows.Next() {
			var createStmt string
			if err := rows.Scan(&createStmt); err != nil {
				return fmt.Errorf("error scanning sqlite row: %w", err)
			}
			formattedStmt := strings.Replace(createStmt, "CREATE TABLE ", "table ", 1)
			formattedStmt = strings.Replace(formattedStmt, "CREATE UNIQUE INDEX", "UNIQUE", 1)
			formattedStmt = strings.Replace(formattedStmt, "CREATE INDEX", "INDEX", 1)

			if strings.Contains(formattedStmt, "(") {
				openParen := strings.Index(formattedStmt, "(")
				closeParen := strings.LastIndex(formattedStmt, ")")

				if openParen != -1 && closeParen > openParen {
					tableNamePart := formattedStmt[:openParen+1]
					columnsPart := formattedStmt[openParen+1 : closeParen]
					restOfStmt := formattedStmt[closeParen:]
					columnDefs := splitOnTopLevelCommas(columnsPart)

					for i, colDef := range columnDefs {
						columnDefs[i] = "\t" + strings.TrimSpace(colDef)
					}
					formattedColumns := strings.Join(columnDefs, ",\n")
					formattedStmt = tableNamePart + "\n" + formattedColumns + "\n" + restOfStmt
				}
			}
			tableSchemas = append(tableSchemas, formattedStmt)
		}
		schema = strings.Join(tableSchemas, "\n\n")
	case "postgres":
		rows, err := conn.QueryContext(ctx, `
			SELECT
				c.table_name,
				c.column_name,
				c.data_type,
				c.udt_name,
				c.character_maximum_length,
				c.is_nullable,
				pg_get_expr(ad.adbin, ad.adrelid) AS column_default,
				EXISTS (
					SELECT 1
					FROM information_schema.table_constraints tc
					JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
					WHERE tc.table_schema = c.table_schema
					AND tc.table_name = c.table_name
					AND ccu.column_name = c.column_name
					AND tc.constraint_type = 'PRIMARY KEY'
				) AS is_primary_key,
				EXISTS (
					SELECT 1
					FROM information_schema.table_constraints tc
					JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
					WHERE tc.table_schema = c.table_schema
					AND tc.table_name = c.table_name
					AND ccu.column_name = c.column_name
					AND tc.constraint_type = 'UNIQUE'
				) AS is_unique
			FROM
				information_schema.columns c
			LEFT JOIN
				pg_attribute a ON a.attrelid = (SELECT oid FROM pg_class WHERE relname = c.table_name AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = c.table_schema))
				AND a.attname = c.column_name
			LEFT JOIN
				pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
			WHERE
				c.table_schema = 'public'
			ORDER BY
				c.table_name, c.ordinal_position;
		`)
		if err != nil {
			return fmt.Errorf("error querying postgres information_schema: %w", err)
		}
		defer rows.Close()

		var currentTable string
		tableColumnsMap := make(map[string][]string)
		tableOrder := []string{}

		for rows.Next() {
			var tableName, columnName, dataType, udtName, isNullable, columnDefault sql.NullString
			var characterMaximumLength sql.NullInt64
			var isPrimaryKey bool
			var isUnque bool

			if err := rows.Scan(&tableName, &columnName, &dataType, &udtName, &characterMaximumLength, &isNullable, &columnDefault, &isPrimaryKey, &isUnque); err != nil {
				return fmt.Errorf("error scanning postgres row: %w", err)
			}

			if !tableName.Valid || tableName.String == "_schema_migrations" {
				continue
			}

			if tableName.String != currentTable {
				if _, exists := tableColumnsMap[tableName.String]; !exists {
					tableOrder = append(tableOrder, tableName.String)
				}
				currentTable = tableName.String
			}

			if !columnName.Valid {
				continue
			}

			columnDef := fmt.Sprintf("  %s", columnName.String)

			displayType := dataType.String
			if udtName.Valid {
				if (udtName.String == "int4" || udtName.String == "int8") && columnDefault.Valid && strings.Contains(columnDefault.String, "nextval(") {
					displayType = "SERIAL"
				} else if dataType.String == "character varying" && characterMaximumLength.Valid {
					displayType = fmt.Sprintf("VARCHAR(%d)", characterMaximumLength.Int64)
				} else if dataType.String == "character" && characterMaximumLength.Valid {
					displayType = fmt.Sprintf("CHAR(%d)", characterMaximumLength.Int64)
				} else if dataType.String == "text" {
					displayType = "TEXT"
				}
			}

			columnDef += fmt.Sprintf(" %s", displayType)
			if isNullable.Valid && isNullable.String == "NO" {
				columnDef += " NOT NULL"
			}
			if columnDefault.Valid && columnDefault.String != "" && !strings.Contains(columnDefault.String, "nextval(") {
				defaultValue := columnDefault.String
				defaultValue = strings.Split(defaultValue, "::")[0]
				columnDef += fmt.Sprintf(" DEFAULT %s", defaultValue)
			}
			if isPrimaryKey {
				columnDef += " PRIMARY KEY"
			}
			if isUnque {
				columnDef += " UNIQUE"
			}
			tableColumnsMap[tableName.String] = append(tableColumnsMap[tableName.String], columnDef)
		}

		fkRows, err := conn.QueryContext(ctx, `
     SELECT
       kcu.table_name AS from_table,
       kcu.column_name AS from_column,
       ccu.table_name AS to_table,
       ccu.column_name AS to_column,
       rc.delete_rule,
       rc.update_rule
     FROM
       information_schema.table_constraints AS tc
       JOIN information_schema.key_column_usage AS kcu
         ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
       JOIN information_schema.referential_constraints AS rc
         ON tc.constraint_name = rc.constraint_name
         AND tc.table_schema = rc.constraint_schema
       JOIN information_schema.constraint_column_usage AS ccu
         ON rc.unique_constraint_name = ccu.constraint_name
         AND rc.constraint_schema = ccu.constraint_schema
     WHERE tc.constraint_type = 'FOREIGN KEY'
     AND tc.table_schema = 'public'
     ORDER BY from_table, from_column;
`)
		if err != nil {
			return fmt.Errorf("error querying postgres foreign keys: %w", err)
		}
		defer fkRows.Close()

		foreignKeys := make(map[string][]ForeignKey)
		for fkRows.Next() {
			var fk ForeignKey
			if err := fkRows.Scan(&fk.TableName, &fk.ColumnName, &fk.ForeignTableName, &fk.ForeignColumnName, &fk.OnDelete, &fk.OnUpdate); err != nil {
				return fmt.Errorf("error scanning foreign key row: %w", err)
			}
			if fk.TableName != "_schema_migrations" {
				foreignKeys[fk.TableName] = append(foreignKeys[fk.TableName], fk)
			}
		}

		var tableDefinitions []string
		for _, tableName := range tableOrder {
			if tableName == "_schema_migrations" {
				continue
			}
			columns := tableColumnsMap[tableName]
			var columnList []string
			for _, columnDef := range columns {
				parts := strings.Fields(columnDef)
				columnName := parts[0]
				if fks, ok := foreignKeys[tableName]; ok {
					for _, fk := range fks {
						if fk.ColumnName == columnName {
							columnDef = fmt.Sprintf("  %s %s REFERENCES %s(%s)",
								fk.ColumnName, parts[1], fk.ForeignTableName, fk.ForeignColumnName)
							if fk.OnDelete != "" && fk.OnDelete != "NO ACTION" {
								columnDef += fmt.Sprintf(" ON DELETE %s", fk.OnDelete)
							}
							if fk.OnUpdate != "" && fk.OnUpdate != "NO ACTION" {
								columnDef += fmt.Sprintf(" ON UPDATE %s", fk.OnUpdate)
							}
						}
					}
				}
				columnList = append(columnList, columnDef)
			}
			tableDefinitions = append(tableDefinitions, fmt.Sprintf("table %s (\n%s\n)", tableName, strings.Join(columnList, ",\n")))
		}
		schema = strings.Join(tableDefinitions, "\n\n")
	case "mysql", "mariadb":
		rows, err := conn.QueryContext(ctx, "SHOW TABLES")
		if err != nil {
			return fmt.Errorf("error querying mysql tables: %w", err)
		}
		defer rows.Close()

		var tableNames []string
		for rows.Next() {
			var tableName string
			if err := rows.Scan(&tableName); err != nil {
				return fmt.Errorf("error scanning mysql table name: %s, %v", tableName, err)
			}
			tableNames = append(tableNames, tableName)
		}

		var tableSchemas []string
		re := regexp.MustCompile(`\s*ENGINE=InnoDB.*(?:DEFAULT)?\s*CHARSET=[^\s]+(?: COLLATE=[^\s]+)?;?`)

		for _, tableName := range tableNames {
			if tableName == "_schema_migrations" {
				continue
			}
			var createTableSQL, dummyTableName string
			row := conn.QueryRow(fmt.Sprintf("SHOW CREATE TABLE %s", tableName))
			if err := row.Scan(&dummyTableName, &createTableSQL); err != nil {
				return fmt.Errorf("error getting SHOW CREATE TABLE for %s: %w", tableName, err)
			}
			createTableSQL = strings.ReplaceAll(createTableSQL, "`", "")
			modifiedStmt := strings.Replace(createTableSQL, "CREATE TABLE ", "table ", 1)
			modifiedStmt = re.ReplaceAllString(modifiedStmt, "")
			tableSchemas = append(tableSchemas, modifiedStmt)
		}
		schema = strings.Join(tableSchemas, "\n\n")
	default:
		return fmt.Errorf("PullDBSchema: unsupported database type: %s", dbtype)
	}

	newSchemaContent := strings.ReplaceAll(schema, "\r\n", "\n")
	newSchemaContent = strings.ReplaceAll(newSchemaContent, "\r", "")

	var configLines []string
	var foundSchemaStart bool

	file, err := os.Open(schemaFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println("Schema file not found, creating new one.")
			return os.WriteFile(schemaFilePath, []byte(newSchemaContent), 0644)
		}
		return fmt.Errorf("error opening existing schema file %s: %w", schemaFilePath, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "db") || strings.HasPrefix(line, "url") || !foundSchemaStart {
			if !strings.Contains(line, "CREATE TABLE") && !strings.Contains(line, "table ") && !strings.Contains(line, "PRIMARY KEY") {
				configLines = append(configLines, line)
			} else {
				foundSchemaStart = true
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading schema file: %w", err)
	}

	var cleanConfigLines []string
	for _, line := range configLines {
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			cleanConfigLines = append(cleanConfigLines, trimmed)
		}
	}

	var combinedContent string
	if len(cleanConfigLines) > 0 {
		combinedContent = strings.Join(cleanConfigLines, "\n") + "\n\n"
	}
	combinedContent += newSchemaContent

	f, err := os.OpenFile(schemaFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("error opening schema file %s for writing: %w", schemaFilePath, err)
	}
	defer f.Close()

	if _, err := f.WriteString(combinedContent); err != nil {
		return fmt.Errorf("error writing combined schema to file %s: %w", schemaFilePath, err)
	}

	fmt.Printf("Database schema written to %s\n", schemaFilePath)
	return nil
}

func truncate(s string, max int) string {
	s = strings.ReplaceAll(s, "\r\n", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	runes := []rune(s)
	if len(runes) > max {
		limit := max - 1
		if limit < 0 {
			limit = 0
		}
		return string(runes[:limit]) + "…"
	}
	return s
}

func printTable(headers []string, data [][]string) string {
	if len(headers) == 0 {
		return ""
	}
	const maxColWidth = 50
	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = min(len(h), maxColWidth)
	}
	for _, row := range data {
		for i, cell := range row {
			if i < len(widths) {
				l := min(
					len(cell), maxColWidth)

				if l > widths[i] {
					widths[i] = l
				}
			}
		}
	}
	buildSeparator := func() string {
		var sb strings.Builder
		sb.WriteString("+")
		for _, w := range widths {
			sb.WriteString(strings.Repeat("-", w+2))
			sb.WriteString("+")
		}
		return sb.String()
	}
	var sb strings.Builder
	separator := buildSeparator()
	sb.WriteString(separator + "\n")
	sb.WriteString("|")
	for i, h := range headers {
		val := truncate(h, widths[i])
		fmt.Fprintf(&sb, " %-*s |", widths[i], val)
	}
	sb.WriteString("\n" + separator + "\n")
	for _, row := range data {
		sb.WriteString("|")
		for i, cell := range row {
			if i < len(widths) {
				val := truncate(cell, widths[i])
				fmt.Fprintf(&sb, " %-*s |", widths[i], val)
			}
		}
		sb.WriteString("\n")
	}
	sb.WriteString(separator)
	return sb.String()
}

type keyMap struct {
	Tab        key.Binding
	Search     key.Binding
	Clear      key.Binding
	RunQuery   key.Binding
	Navigation key.Binding
	Enter      key.Binding
	Quit       key.Binding
}

func (k keyMap) ShortHelp() []key.Binding {
	return []key.Binding{k.Tab, k.Search, k.Clear, k.RunQuery, k.Navigation, k.Enter, k.Quit}
}
func (k keyMap) FullHelp() [][]key.Binding { return nil }

var keys = keyMap{
	Tab:        key.NewBinding(key.WithKeys("tab"), key.WithHelp("tab", "focus next")),
	Search:     key.NewBinding(key.WithKeys("/"), key.WithHelp("/", "search")),
	Clear:      key.NewBinding(key.WithKeys("esc"), key.WithHelp("esc", "clear input")),
	RunQuery:   key.NewBinding(key.WithKeys("f5"), key.WithHelp("f5", "run query")),
	Navigation: key.NewBinding(key.WithKeys("up", "down", "left", "right", "k", "j", "h", "l"), key.WithHelp("↑/↓/←/→", "nav")),
	Enter:      key.NewBinding(key.WithKeys("enter"), key.WithHelp("enter", "select")),
	Quit:       key.NewBinding(key.WithKeys("ctrl+c"), key.WithHelp("ctrl+c", "quit")),
}

var (
	appStyle           = lipgloss.NewStyle()
	inputStyle         = lipgloss.NewStyle().Border(lipgloss.NormalBorder()).BorderForeground(lipgloss.Color("240"))
	tableListPaneStyle = lipgloss.NewStyle().Border(lipgloss.NormalBorder(), false, true, false, false).BorderForeground(lipgloss.Color("240"))
	tableDataPaneStyle = lipgloss.NewStyle().PaddingLeft(1)
	errorStyle         = lipgloss.NewStyle().Foreground(lipgloss.Color("9")).Bold(true)
	successStyle       = lipgloss.NewStyle().Foreground(lipgloss.Color("10")).Bold(true)
	footerStyle        = lipgloss.NewStyle().MarginTop(1).Padding(0, 1)
	titleStyle         = lipgloss.NewStyle()
	itemStyle          = lipgloss.NewStyle().PaddingLeft(2)
	selectedItemStyle  = lipgloss.NewStyle().PaddingLeft(0).Foreground(lipgloss.Color("170"))
	tableSelectedStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("229")).Background(lipgloss.Color("57"))
)

type tableItem string

func (t tableItem) FilterValue() string { return string(t) }

type itemDelegate struct{}

func (d itemDelegate) Height() int                             { return 1 }
func (d itemDelegate) Spacing() int                            { return 0 }
func (d itemDelegate) Update(_ tea.Msg, _ *list.Model) tea.Cmd { return nil }
func (d itemDelegate) Render(w io.Writer, m list.Model, index int, listItem list.Item) {
	i, ok := listItem.(tableItem)
	if !ok {
		return
	}
	str := string(i)
	fn := itemStyle.Render
	if index == m.Index() {
		fn = func(s ...string) string {
			return selectedItemStyle.Render("> " + strings.Join(s, " "))
		}
	}
	fmt.Fprint(w, fn(str))
}

type model struct {
	db                 *sql.DB
	dbType             string
	sqlTextarea        textarea.Model
	viewport           viewport.Model
	tableList          list.Model
	help               help.Model
	keys               keyMap
	focusedPane        int
	selectedTable      string
	queryError         error
	showingQueryResult bool
	width              int
	height             int
	table              table.Model
	viewportPaneWidth  int
	viewportPaneHeight int
	viewingRowDetail   bool
	selectedRow        []string
	tableXOffset       int
}

func initialModel(db *sql.DB, dbType string) model {
	tables, err := getSQLTables(db, dbType)
	if err != nil {
		log.Fatalf("Error getting SQL tables: %v", err)
	}

	items := make([]list.Item, len(tables))
	for i, t := range tables {
		items[i] = tableItem(t)
	}

	tl := list.New(items, itemDelegate{}, 0, 0)
	tl.Title = "Tables"
	tl.SetShowStatusBar(false)
	tl.SetShowHelp(false)
	tl.DisableQuitKeybindings()
	tl.Styles.Title = titleStyle

	ta := textarea.New()
	ta.Placeholder = "Input SQL query"
	ta.Focus()
	ta.ShowLineNumbers = false
	ta.SetHeight(5)
	ta.Prompt = " "

	vp := viewport.New(viewport.WithWidth(80), viewport.WithHeight(20))
	vp.SetContent("Select a table to view its data or run a query.")

	h := help.New()

	t := table.New(table.WithFocused(true), table.WithHeight(7))
	s := table.DefaultStyles()
	s.Selected = tableSelectedStyle
	t.SetStyles(s)

	return model{
		db:          db,
		dbType:      dbType,
		sqlTextarea: ta,
		viewport:    vp,
		tableList:   tl,
		help:        h,
		keys:        keys,
		focusedPane: 0,
		table:       t,
	}
}

func (m model) Init() tea.Cmd { return textarea.Blink }

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var (
		cmd  tea.Cmd
		cmds []tea.Cmd
	)

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.help.SetWidth(msg.Width)
		footerHeight := lipgloss.Height(footerStyle.Render(m.help.View(m.keys)))
		mainHeight := m.height - appStyle.GetVerticalFrameSize() - footerHeight
		listWidth := 24
		m.tableList.SetSize(listWidth, mainHeight)
		rightColumnWidth := m.width - listWidth - appStyle.GetHorizontalPadding()
		inputWidth := rightColumnWidth - inputStyle.GetHorizontalFrameSize()
		m.sqlTextarea.SetWidth(inputWidth)
		inputHeight := lipgloss.Height(inputStyle.Render(m.sqlTextarea.View()))
		m.viewportPaneHeight = mainHeight - inputHeight - tableDataPaneStyle.GetVerticalFrameSize()
		m.viewportPaneWidth = rightColumnWidth - tableDataPaneStyle.GetHorizontalFrameSize()
		m.table.SetHeight(m.viewportPaneHeight)
		if m.viewingRowDetail {
			m.viewport.SetWidth(m.width)
			m.viewport.SetHeight(m.height)
			m.renderDetailView()
		} else {
			m.viewport.SetWidth(m.viewportPaneWidth)
			m.viewport.SetHeight(m.viewportPaneHeight)
			if m.showingQueryResult || len(m.table.Columns()) > 0 {
				m.viewport.SetContent(m.table.View())
			}
		}
		return m, nil

	case tea.KeyMsg:
		switch {
		case key.Matches(msg, m.keys.Quit):
			return m, tea.Quit
		case key.Matches(msg, m.keys.Tab):
			m.focusedPane = (m.focusedPane + 1) % 3
			if m.focusedPane == 0 {
				m.sqlTextarea.Focus()
			} else {
				m.sqlTextarea.Blur()
			}
			return m, nil
		case key.Matches(msg, m.keys.RunQuery):
			if m.focusedPane == 0 {
				query := m.sqlTextarea.Value()
				if query != "" {
					m.executeSQLQuery(query)
				}
			}
			return m, nil
		}
		if m.tableList.FilterState() == list.Filtering {
			m.tableList, cmd = m.tableList.Update(msg)
			return m, cmd
		}
		if m.viewingRowDetail {
			switch {
			case key.Matches(msg, m.keys.Clear), key.Matches(msg, m.keys.Quit), msg.String() == "esc", msg.String() == "q":
				m.viewingRowDetail = false
				m.viewport.SetWidth(m.viewportPaneWidth)
				m.viewport.SetHeight(m.viewportPaneHeight)
				m.viewport.SetContent(m.table.View())
				return m, nil
			default:
				m.viewport, cmd = m.viewport.Update(msg)
				cmds = append(cmds, cmd)
				return m, tea.Batch(cmds...)
			}
		} else {
			switch m.focusedPane {
			case 0:
				switch {
				case key.Matches(msg, m.keys.Clear):
					m.sqlTextarea.SetValue("")
				default:
					m.sqlTextarea, cmd = m.sqlTextarea.Update(msg)
				}
				cmds = append(cmds, cmd)

			case 1:
				switch {
				case key.Matches(msg, m.keys.Enter):
					if i := m.tableList.SelectedItem(); i != nil {
						m.selectedTable = string(i.(tableItem))
						m.showingQueryResult = false
						m.loadTableData(m.selectedTable)
					}
				}
				m.tableList, cmd = m.tableList.Update(msg)
				cmds = append(cmds, cmd)

			case 2:
				if m.viewingRowDetail {
					switch {
					case key.Matches(msg, m.keys.Clear), key.Matches(msg, m.keys.Quit), msg.String() == "esc":
						m.viewingRowDetail = false
						m.selectedRow = nil
						m.viewport.SetWidth(m.viewportPaneWidth)
						m.viewport.SetHeight(m.viewportPaneHeight)
						m.viewport.SetContent(m.table.View())
						m.viewport.SetXOffset(m.tableXOffset)
						return m, nil
					default:
						m.viewport, cmd = m.viewport.Update(msg)
						cmds = append(cmds, cmd)
					}
				} else {
					switch {
					case key.Matches(msg, m.keys.Enter):
						row := m.table.SelectedRow()
						if row == nil {
							return m, nil
						}
						m.selectedRow = row
						m.viewingRowDetail = true
						m.viewport.SetWidth(m.width)
						m.viewport.SetHeight(m.height)
						m.renderDetailView()
						m.tableXOffset = m.viewport.XOffset()
						m.viewport.GotoTop()
						m.viewport.SetXOffset(0)
						return m, nil

					case msg.String() == "left", msg.String() == "h":
						m.viewport.ScrollLeft(5)
					case msg.String() == "right", msg.String() == "l":
						m.viewport.ScrollRight(5)
					case key.Matches(msg, m.keys.Navigation):
						m.table, cmd = m.table.Update(msg)
						cmds = append(cmds, cmd)
						m.viewport.SetContent(m.table.View())
					default:
						m.viewport, cmd = m.viewport.Update(msg)
						cmds = append(cmds, cmd)
					}
				}
			}
		}

	default:
		// Default updates
		m.sqlTextarea, cmd = m.sqlTextarea.Update(msg)
		cmds = append(cmds, cmd)
		m.tableList, cmd = m.tableList.Update(msg)
		cmds = append(cmds, cmd)
		m.viewport, cmd = m.viewport.Update(msg)
		cmds = append(cmds, cmd)
	}
	return m, tea.Batch(cmds...)
}

func (m model) View() tea.View {
	if m.viewingRowDetail {
		v := tea.NewView(appStyle.Render(m.viewport.View()))
		v.AltScreen = true
		return v
	}

	var listStyle lipgloss.Style
	dataStyle := tableDataPaneStyle
	currentInputStyle := inputStyle

	switch m.focusedPane {
	case 0:
		currentInputStyle = inputStyle.BorderForeground(lipgloss.Color("170"))
		listStyle = tableListPaneStyle
	case 1:
		listStyle = tableListPaneStyle.BorderForeground(lipgloss.Color("170"))
	case 2:
		listStyle = tableListPaneStyle
		dataStyle = tableDataPaneStyle.Border(lipgloss.NormalBorder(), false, false, false, true).BorderForeground(lipgloss.Color("170"))
	default:
		listStyle = tableListPaneStyle
	}

	inputView := currentInputStyle.Render(m.sqlTextarea.View())
	finalTableListContent := listStyle.Render(m.tableList.View())
	finalTableDataContent := dataStyle.Render(m.viewport.View())
	footerView := footerStyle.Render(m.help.View(m.keys))
	rightColumn := lipgloss.JoinVertical(lipgloss.Left, inputView, finalTableDataContent)
	mainContent := lipgloss.JoinHorizontal(lipgloss.Top, finalTableListContent, rightColumn)
	v := tea.NewView(appStyle.Render(lipgloss.JoinVertical(lipgloss.Left, mainContent, footerView)))
	v.AltScreen = false
	return v
}
func (m *model) renderDetailView() {
	if len(m.selectedRow) == 0 {
		return
	}

	headers := m.table.Columns()
	var sb strings.Builder

	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("229")).Underline(true)
	sb.WriteString(titleStyle.Render("Row Details") + "\n\n")
	wrapWidth := max(m.width-4, 10)

	for i, cell := range m.selectedRow {
		header := "Unknown"
		if i < len(headers) {
			header = headers[i].Title
		}

		hStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("39")).Bold(true)
		vStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("255")).Width(wrapWidth)

		sb.WriteString(hStyle.Render(header + ":"))
		sb.WriteString("\n")
		sb.WriteString(vStyle.Render(cell))
		sb.WriteString("\n\n")
	}

	sb.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("240")).Render("\n(Press Esc or q to close)"))

	m.viewport.SetContent(sb.String())
}

func (m *model) loadTableData(tableName string) error {
	m.viewingRowDetail = false
	m.queryError = nil
	m.showingQueryResult = false
	m.table.SetCursor(0)
	m.viewport.GotoTop()
	m.viewport.SetXOffset(0)
	m.table.SetRows(nil)

	rows, err := m.db.Query(fmt.Sprintf("SELECT * FROM %s LIMIT 50;", tableName))
	if err != nil {
		return fmt.Errorf("failed to query data: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}

	widths := make([]int, len(columns))
	for i, col := range columns {
		widths[i] = len(col)
	}

	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]any, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	var tableRows []table.Row

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		var row []string
		for i, colVal := range values {
			var valStr string
			if colVal == nil {
				valStr = "NULL"
			} else {
				valStr = string(colVal)
			}

			if len(valStr) > widths[i] {
				widths[i] = len(valStr)
			}

			row = append(row, valStr)
		}
		tableRows = append(tableRows, row)
	}

	const maxColWidth = 50

	var tableColumns []table.Column
	totalWidth := 0

	for i, colName := range columns {
		w := min(widths[i], maxColWidth)
		tableColumns = append(tableColumns, table.Column{Title: colName, Width: w})
		totalWidth += w
	}

	m.table.SetColumns(tableColumns)
	m.table.SetRows(tableRows)
	if totalWidth < m.viewport.Width() {
		m.table.SetWidth(m.viewport.Width())
	} else {
		m.table.SetWidth(totalWidth)
	}

	m.viewport.SetContent(m.table.View())
	return nil
}

func (m *model) executeSQLQuery(query string) {
	m.viewingRowDetail = false
	m.queryError = nil
	m.showingQueryResult = true
	m.table.SetCursor(0)
	m.viewport.GotoTop()
	m.viewport.SetXOffset(0)
	m.table.SetRows(nil)

	rows, err := m.db.Query(query)
	if err != nil {
		m.queryError = err
		m.viewport.SetContent(errorStyle.Render(fmt.Sprintf("SQL Error:\n%v", m.queryError.Error())))
		return
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		m.queryError = fmt.Errorf("failed to get columns: %w", err)
		m.viewport.SetContent(errorStyle.Render(m.queryError.Error()))
		return
	}

	widths := make([]int, len(cols))
	for i, col := range cols {
		widths[i] = len(col)
	}

	values := make([]sql.RawBytes, len(cols))
	scanArgs := make([]any, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	var tableRows []table.Row
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			m.queryError = fmt.Errorf("failed to scan row: %w", err)
			m.viewport.SetContent(errorStyle.Render(m.queryError.Error()))
			return
		}

		var row []string
		for i, colVal := range values {
			var valStr string
			if colVal == nil {
				valStr = "NULL"
			} else {
				valStr = string(colVal)
			}

			if len(valStr) > widths[i] {
				widths[i] = len(valStr)
			}

			row = append(row, valStr)
		}
		tableRows = append(tableRows, row)
	}

	if err := rows.Err(); err != nil {
		m.queryError = fmt.Errorf("rows iteration error: %w", err)
		m.viewport.SetContent(errorStyle.Render(m.queryError.Error()))
		return
	}

	const maxColWidth = 50

	var tableColumns []table.Column
	totalWidth := 0

	for i, colName := range cols {
		w := min(widths[i], maxColWidth)
		tableColumns = append(tableColumns, table.Column{Title: colName, Width: w})
		totalWidth += w
	}

	m.table.SetColumns(tableColumns)
	m.table.SetRows(tableRows)

	if totalWidth < m.viewport.Width() {
		m.table.SetWidth(m.viewport.Width())
	} else {
		m.table.SetWidth(totalWidth)
	}

	successMsg := successStyle.Render("Query executed successfully")
	m.viewport.SetContent(successMsg + "\n" + m.table.View())
}

func getSQLTables(db *sql.DB, dbType string) ([]string, error) {
	dialect := GetDialect(dbType)
	if dialect.ListTables == "" {
		return nil, fmt.Errorf("unsupported database type for listing tables: %s", dbType)
	}

	rows, err := db.Query(dialect.ListTables)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables (%s): %w", dbType, err)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name (%s): %w", dbType, err)
		}
		tables = append(tables, tableName)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error (%s): %w", dbType, err)
	}
	return tables, nil
}

func getSQLColumns(db *sql.DB, dbType string, tableName string) ([]string, error) {
	var cols []string
	dialect := GetDialect(dbType)
	if dialect.ListCols == "" {
		return nil, fmt.Errorf("unsupported database type for loading table data: %s", dbType)
	}

	rows, err := db.Query(dialect.ListCols, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get table info for %s (%s): %w", tableName, dbType, err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan column info (%s): %w", dbType, err)
		}
		cols = append(cols, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error on getSQLColumns (%s): %w", dbType, err)
	}
	return cols, nil
}

type Dialect struct{ Type, TableExists, CreateInit, Insert, Update, Delete, SelectStatus, ListTables, ListCols string }

func GetDialect(dbType string) Dialect {
	switch dbType {
	case "postgres":
		return Dialect{
			Type:         dbType,
			TableExists:  "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename = '_schema_migrations'",
			CreateInit:   "CREATE TABLE IF NOT EXISTS _schema_migrations (\n  id SERIAL PRIMARY KEY, \n  file VARCHAR(255) UNIQUE,\n  migrated BOOLEAN DEFAULT false\n);",
			Insert:       "INSERT INTO _schema_migrations (file, migrated) VALUES ($1, $2)",
			Update:       "UPDATE _schema_migrations SET migrated = $1 WHERE file = $2",
			Delete:       "DELETE FROM _schema_migrations WHERE file = $1",
			SelectStatus: "SELECT migrated FROM _schema_migrations WHERE file = $1",
			ListTables:   "SELECT tablename FROM pg_tables WHERE schemaname = 'public';",
			ListCols:     "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 ORDER BY ordinal_position;",
		}
	case "sqlite", "libsql", "turso":
		return Dialect{
			Type:         dbType,
			TableExists:  "SELECT name FROM sqlite_master WHERE type='table' AND name='_schema_migrations'",
			CreateInit:   "CREATE TABLE IF NOT EXISTS _schema_migrations (\n  id INTEGER PRIMARY KEY AUTOINCREMENT, \n  file VARCHAR(255) UNIQUE,\n  migrated BOOLEAN DEFAULT false\n);",
			Insert:       "INSERT INTO _schema_migrations (file, migrated) VALUES (?, ?)",
			Update:       "UPDATE _schema_migrations SET migrated = ? WHERE file = ?",
			Delete:       "DELETE FROM _schema_migrations WHERE file = ?",
			SelectStatus: "SELECT migrated FROM _schema_migrations WHERE file = ?",
			ListTables:   "SELECT name FROM sqlite_master WHERE type='table';",
			ListCols:     "SELECT name FROM PRAGMA_TABLE_INFO(?);",
		}
	case "mysql", "mariadb":
		return Dialect{
			Type:         dbType,
			TableExists:  "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '_schema_migrations'",
			CreateInit:   "CREATE TABLE IF NOT EXISTS _schema_migrations (\n  id INT PRIMARY KEY AUTO_INCREMENT, \n  file VARCHAR(255) UNIQUE,\n  migrated BOOLEAN DEFAULT false\n);",
			Insert:       "INSERT INTO _schema_migrations (file, migrated) VALUES (?, ?)",
			Update:       "UPDATE _schema_migrations SET migrated = ? WHERE file = ?",
			Delete:       "DELETE FROM _schema_migrations WHERE file = ?",
			SelectStatus: "SELECT migrated FROM _schema_migrations WHERE file = ?",
			ListTables:   "SHOW TABLES;",
			ListCols:     "SELECT column_name FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? ORDER BY ordinal_position;",
		}
	}
	return Dialect{}
}
