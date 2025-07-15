package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/lipgloss/table"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/joho/godotenv"
	_ "modernc.org/sqlite"
)

var version = "dev"

func main() {
	var migrate MigrateFlag
	flag.Var(&migrate, "migrate", "migrate database")
	v := flag.Bool("v", false, "version")
	i := flag.Bool("i", false, "init schema files")
	db := flag.String("db", "sqlite", "add db: sqlite, postgres, mysql, mariadb")
	url := flag.String("url", "./schema/dev.db", "add dburl")
	create := flag.String("create", "", "create sql file name")
	pull := flag.Bool("pull", false, "get database schema")
	sql := flag.String("sql", "", "run sql commands")
	dir := flag.String("dir", "migrations", "choose path under root-directory/")
	rdir := flag.String("rdir", "schema", "root directory")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Path: %s\n", os.Args[0])
		fmt.Fprintln(os.Stderr, "A language agnostic CLI tool for handling database migrations")
		fmt.Fprintln(os.Stderr, "\nOptions:")
		flag.PrintDefaults()
		fmt.Fprintln(os.Stderr, "\nExamples:")
		fmt.Fprintln(os.Stderr, "  schema -i")
		fmt.Fprintln(os.Stderr, `  schema -i -db="postgres" -url="postgresql://postgres:postgres@localhost:5432/postgres"`)
		fmt.Fprintln(os.Stderr, `  schema -create="createuser"`)
		fmt.Fprintln(os.Stderr, "  schema -migrate")
		fmt.Fprintln(os.Stderr, `  schema -migrate="1_createuser"`)
		fmt.Fprintln(os.Stderr, `  schema -dir="functions" -create="insertusers"`)
		fmt.Fprintln(os.Stderr, `  schema -dir="functions" -migrate="0_insertusers"`)
	}
	flag.Parse()
	if flag.NFlag() == 0 {
		fmt.Printf(`
 ____       _                          
/ ___|  ___| |__   ___ _ __ ___   __ _ 
\___ \ / __| '_ \ / _ \  _   _ \ / _  |
 ___) | (__| | | |  __/ | | | | | (_| |
|____/ \___|_| |_|\___|_| |_| |_|\__,_|
`)
	}

	if *v {
		fmt.Println("Current Version:", version)

		url := "https://api.github.com/repos/gigagrug/schema/releases/latest"
		resp, err := http.Get(url)
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
		fmt.Println("Latest Version:", latestVersion)
		if version != latestVersion {
			fmt.Printf("Outdated! Update to latest version: %s\n", latestVersion)
		} else {
			fmt.Println("Using latest version")
		}
	}

	schemaPath := fmt.Sprintf("./%s/db.schema", *rdir)

	if *i {
		if !flagUsed("url") && *db == "sqlite" {
			flag.Set("url", fmt.Sprintf("./%s/dev.db", *rdir))
		}
		if _, err := os.Stat(schemaPath); os.IsNotExist(err) {
			err := os.Mkdir(fmt.Sprintf("./%s/", *rdir), 0700)
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

		if _, err := os.Stat("./.env"); os.IsNotExist(err) {
			envFile, err := os.Create("./.env")
			if err != nil {
				log.Fatalf("Error creating .env file: %v\n", err)
			}
			defer envFile.Close()

			schemaContent := fmt.Sprintf(`%s_DB_URL="%s"`, strings.ToUpper(*rdir), *url)
			_, err = envFile.WriteString(schemaContent)
			if err != nil {
				log.Fatalf("Error writing to .env file: %v\n", err)
			}
		} else {
			envFile, err := os.OpenFile("./.env", os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				log.Fatalf("Error opening .env file: %v\n", err)
			}
			defer envFile.Close()

			schemaContent := fmt.Sprintf("\n%s_DB_URL=\"%s\"", strings.ToUpper(*rdir), *url)
			_, err = envFile.WriteString(schemaContent)
			if err != nil {
				log.Fatalf("Error appending to .env file: %v\n", err)
			}
		}
		fmt.Println("Schema successfully initialized")
		return
	}

	if flagUsed("url") {
		upperRdir := strings.ToUpper(*rdir)
		if _, err := os.Stat("./.env"); os.IsNotExist(err) {
			envFile, err := os.Create("./.env")
			if err != nil {
				log.Fatalf("Error creating .env file: %v\n", err)
			}
			defer envFile.Close()

			schemaContent := fmt.Sprintf(`%s_DB_URL="%s"`, upperRdir, *url)
			_, err = envFile.WriteString(schemaContent)
			if err != nil {
				log.Fatalf("Error writing to .env file: %v\n", err)
			}
		} else {
			envFile, err := os.OpenFile("./.env", os.O_RDWR, 0600)
			if err != nil {
				log.Fatalf("Error opening .env file: %v\n", err)
			}
			defer envFile.Close()

			scanner := bufio.NewScanner(envFile)
			var lines []string
			var found bool
			for scanner.Scan() {
				line := scanner.Text()
				if strings.HasPrefix(line, fmt.Sprintf("%s_DB_URL=", upperRdir)) {
					lines = append(lines, fmt.Sprintf(`%s_DB_URL="%s"`, upperRdir, *url))
					found = true
				} else {
					lines = append(lines, line)
				}
			}
			if !found {
				lines = append(lines, fmt.Sprintf(`%s_DB_URL="%s"`, upperRdir, *url))
			}

			envFile.Seek(0, 0)
			envFile.Truncate(0)
			for _, line := range lines {
				_, err := envFile.WriteString(line + "\n")
				if err != nil {
					log.Fatalf("Error writing to .env file: %v\n", err)
				}
			}
		}
	}

	if flagUsed("db") {
		if _, err := os.Stat(schemaPath); err == nil {
			file, err := os.OpenFile(schemaPath, os.O_RDWR, 0600)
			if err != nil {
				log.Fatalf("Error opening db.schema file: %v\n", err)
			}
			defer file.Close()

			scanner := bufio.NewScanner(file)
			var lines []string
			var found bool
			for scanner.Scan() {
				line := scanner.Text()
				if strings.HasPrefix(line, "db = ") {
					lines = append(lines, fmt.Sprintf(`db = "%s"`, *db))
					found = true
				} else {
					lines = append(lines, line)
				}
			}
			if !found {
				lines = append(lines, fmt.Sprintf(`db = "%s"`, *db))
				fmt.Println("db not found")
			}

			file.Seek(0, 0)
			file.Truncate(0)
			for _, line := range lines {
				_, err := file.WriteString(line + "\n")
				if err != nil {
					log.Fatalf("Error writing to db.schema file: %v\n", err)
				}
			}
		} else {
			log.Fatalf("db.schema file does not exist.\n")
		}
		return
	}

	conn, dbtype, err := Conn2DB(schemaPath)
	if err != nil {
		log.Fatalf("SQL Flag: Error connecting to database: %v\n", err)
	}
	defer conn.Close()

	if *pull {
		err = PullDBSchema(conn, dbtype, schemaPath)
		if err != nil {
			log.Fatalf("Err pulling db schema: %v\n", err)
		}
		return
	}

	if *sql != "" {
		if strings.HasSuffix(strings.TrimSpace(*sql), ".sql") {
			fileP := fmt.Sprintf("./%s/%s/%s", *rdir, *dir, *sql)
			sqlFile, err := os.ReadFile(fileP)
			if err != nil {
				log.Fatalf("Error reading SQL file: %v\n", err)
			}
			rows, err := conn.Query(string(sqlFile))
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
				err = rows.Scan(scanArgs...)
				if err != nil {
					log.Fatalf("Error scanning row: %v\n", err)
				}
				row := make([]string, len(columns))
				for i, col := range values {
					if col == nil {
						row[i] = "NULL"
					} else {
						switch v := col.(type) {
						case []byte:
							row[i] = string(v)
						default:
							row[i] = fmt.Sprintf("%v", v)
						}
					}
				}
				data = append(data, row)
			}
			if err = rows.Err(); err != nil {
				log.Fatalf("Error iterating rows: %v\n", err)
			}
			printTable(columns, data)
			err = PullDBSchema(conn, dbtype, schemaPath)
			if err != nil {
				log.Fatalf("Error pulling DB schema after migration: %v\n", err)
			}
			return
		} else if strings.HasPrefix(strings.TrimSpace(strings.ToUpper(*sql)), "SELECT") {
			rows, err := conn.Query(*sql)
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
				err = rows.Scan(scanArgs...)
				if err != nil {
					log.Fatalf("Error scanning row: %v\n", err)
				}
				row := make([]string, len(columns))
				for i, col := range values {
					if col == nil {
						row[i] = "NULL"
					} else {
						switch v := col.(type) {
						case []byte:
							row[i] = string(v)
						default:
							row[i] = fmt.Sprintf("%v", v)
						}
					}
				}
				data = append(data, row)
			}
			if err = rows.Err(); err != nil {
				log.Fatalf("Error iterating rows: %v\n", err)
			}
			printTable(columns, data)
		} else {
			result, err := conn.Exec(*sql)
			if err != nil {
				log.Fatalf("Error executing SQL command: %v\n", err)
			}

			rowsAffected, err := result.RowsAffected()
			if err != nil {
				fmt.Printf("SQL command executed successfully. Could not retrieve rows affected: %v\n", err)
			} else {
				fmt.Printf("SQL command executed successfully. Rows affected: %d\n", rowsAffected)
			}
		}
		return
	}

	if *create != "" {
		if *dir == "migrations" {
			if _, err := os.Stat(fmt.Sprintf("./%s/migrations", *rdir)); os.IsNotExist(err) {
				err = os.Mkdir(fmt.Sprintf("./%s/migrations", *rdir), 0700)
				if err != nil {
					log.Fatalf("Error creating /migrations directory: %v\n", err)
				}
			}
			file, err := os.Create(fmt.Sprintf("./%s/migrations/0_init.sql", *rdir))
			if err != nil {
				log.Fatalf("Error creating 0_init.sql file: %v\n", err)
			}
			defer file.Close()

			var sqlTable string
			switch dbtype {
			case "sqlite":
				sqlTable = "PRAGMA journal_mode=WAL;\n\nCREATE TABLE IF NOT EXISTS _schema_migrations (\n  id INTEGER PRIMARY KEY AUTOINCREMENT, \n  file VARCHAR(255) UNIQUE,\n  migrated BOOLEAN DEFAULT false\n);"
			case "postgres":
				sqlTable = "CREATE TABLE IF NOT EXISTS _schema_migrations (\n  id SERIAL PRIMARY KEY, \n  file VARCHAR(255) UNIQUE,\n  migrated BOOLEAN DEFAULT false\n);"
			case "mysql", "mariadb":
				sqlTable = "CREATE TABLE IF NOT EXISTS _schema_migrations (\n  id INT PRIMARY KEY AUTO_INCREMENT, \n  file VARCHAR(255) UNIQUE,\n  migrated BOOLEAN DEFAULT false\n);"
			default:
				log.Fatalf("Unsupported database type: %s", *db)
			}
			_, err = file.WriteString(sqlTable)
			if err != nil {
				log.Fatalf("Error writing to 0_init.sql file: %v\n", err)
			}
			CheckTableExists(conn, dbtype, *rdir)
		}
		dirPath := fmt.Sprintf("./%s/%s/", *rdir, *dir)
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

		fileCount := 0
		for _, entry := range entries {
			if !entry.IsDir() {
				fileCount++
			}
		}
		fileName := fmt.Sprintf("%d_%s.sql", fileCount, *create)
		schemaFile, err := os.Create(dirPath + fileName)
		if err != nil {
			log.Fatalf("Error creating file: %v\n", err)
		}
		defer schemaFile.Close()

		if *dir == "migrations" {
			var sqlInsert string
			switch dbtype {
			case "sqlite", "postgres":
				sqlInsert = "INSERT INTO _schema_migrations (file) VALUES ($1)"
			case "mysql", "mariadb":
				sqlInsert = "INSERT INTO _schema_migrations (file) VALUES (?)"
			default:
				log.Fatalf("Unsupported database type: %s", dbtype)
			}
			_, err = conn.Exec(sqlInsert, fileName)
			if err != nil {
				log.Fatalf("Error executing SQL: %v\n", err)
			}
		}
		fmt.Printf("Schema successfully created sql file %s\n", fileName)
		return
	}

	if migrate.isSet {
		if _, err := os.Stat(fmt.Sprintf("./%s/migrations", *rdir)); os.IsNotExist(err) {
			err = os.Mkdir(fmt.Sprintf("./%s/migrations", *rdir), 0700)
			if err != nil {
				log.Fatalf("Error creating /migrations directory: %v\n", err)
			}
		}
		file, err := os.Create(fmt.Sprintf("./%s/migrations/0_init.sql", *rdir))
		if err != nil {
			log.Fatalf("Error creating 0_init.sql file: %v\n", err)
		}
		defer file.Close()

		var sqlTable string
		switch *db {
		case "sqlite":
			sqlTable = "PRAGMA journal_mode=WAL;\n\nCREATE TABLE IF NOT EXISTS _schema_migrations (\n  id INTEGER PRIMARY KEY AUTOINCREMENT, \n  file VARCHAR(255) UNIQUE,\n  migrated BOOLEAN DEFAULT false\n);"
		case "postgres":
			sqlTable = "CREATE TABLE IF NOT EXISTS _schema_migrations (\n  id SERIAL PRIMARY KEY, \n  file VARCHAR(255) UNIQUE,\n  migrated BOOLEAN DEFAULT false\n);"
		case "mysql", "mariadb":
			sqlTable = "CREATE TABLE IF NOT EXISTS _schema_migrations (\n  id INT PRIMARY KEY AUTO_INCREMENT, \n  file VARCHAR(255) UNIQUE,\n  migrated BOOLEAN DEFAULT false\n);"
		default:
			log.Fatalf("Unsupported database type: %s", *db)
		}
		_, err = file.WriteString(sqlTable)
		if err != nil {
			log.Fatalf("Error writing to 0_init.sql file: %v\n", err)
		}
		CheckTableExists(conn, dbtype, *rdir)

		migrationsDir := fmt.Sprintf("./%s/migrations", *rdir)
		localMigrationFiles, err := os.ReadDir(migrationsDir)
		if err != nil {
			log.Fatalf("Error reading migrations directory '%s': %v\n", migrationsDir, err)
		}

		dbMigrationFiles := make(map[string]bool)
		rows, err := conn.Query("SELECT file FROM _schema_migrations")
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

		var sqlInsert string
		switch dbtype {
		case "sqlite", "postgres":
			sqlInsert = "INSERT INTO _schema_migrations (file, migrated) VALUES ($1, false)"
		case "mysql", "mariadb":
			sqlInsert = "INSERT INTO _schema_migrations (file, migrated) VALUES (?, false)"
		default:
			log.Fatalf("Unsupported database type for inserting new migration files: %s", dbtype)
		}

		for _, entry := range localMigrationFiles {
			if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") {
				if _, exists := dbMigrationFiles[entry.Name()]; !exists {
					_, err = conn.Exec(sqlInsert, entry.Name())
					if err != nil {
						fmt.Printf("Warning: Could not add migration file '%s' to _schema_migrations table: %v\n", entry.Name(), err)
					} else {
						fmt.Printf("Added new migration file '%s' to _schema_migrations table.\n", entry.Name())
					}
				}
			}
		}

		if migrate.String() != "true" {
			migrationFileName := migrate.String()
			fileP := fmt.Sprintf("./%s/migrations/%s.sql", *rdir, migrationFileName)
			sqlFile, err := os.ReadFile(fileP)
			if err != nil {
				log.Fatalf("Error reading SQL file: %v\n", err)
			}

			_, err = conn.Exec(string(sqlFile))
			if err != nil {
				log.Fatalf("Error executing SQL: %v\n", err)
			}

			var sqlUpdate string
			switch dbtype {
			case "sqlite", "postgres":
				sqlUpdate = "UPDATE _schema_migrations SET migrated = true WHERE file = $1"
			case "mysql", "mariadb":
				sqlUpdate = "UPDATE _schema_migrations SET migrated = true WHERE file = ?"
			default:
				log.Fatalf("Unsupported database type: %s", dbtype)
			}
			_, err = conn.Exec(sqlUpdate, migrationFileName)
			if err != nil {
				log.Fatalf("Error executing SQL: %v\n", err)
			}

			err = PullDBSchema(conn, dbtype, schemaPath)
			if err != nil {
				log.Fatalf("Error pulling DB schema after migration: %v\n", err)
			}
			fmt.Printf("Schema successfully migrated %s\n", migrationFileName)
			return
		} else {
			rows, err := conn.Query(`SELECT file FROM _schema_migrations WHERE migrated = false ORDER BY id ASC`)
			if err != nil {
				log.Fatalf("Error executing SQL query for pending migrations: %v\n", err)
			}
			defer rows.Close()

			type Files struct {
				Name string
			}
			files := []Files{}
			for rows.Next() {
				var file string
				err = rows.Scan(&file)
				if err != nil {
					log.Fatalf("Error scanning row for pending migration file: %v\n", err)
				}
				files = append(files, Files{Name: file})
			}

			if len(files) == 0 {
				fmt.Println("No pending migrations found.")
				return
			}

			for _, entry := range files {
				fileP := fmt.Sprintf("./%s/migrations/%s", *rdir, entry.Name)
				sqlFile, err := os.ReadFile(fileP)
				if err != nil {
					log.Fatalf("Error reading SQL file for migration %s: %v\n", entry.Name, err)
				}

				_, err = conn.Exec(string(sqlFile))
				if err != nil {
					log.Fatalf("Error executing SQL for migration %s: %v\n", entry.Name, err)
				}

				var sqlUpdate string
				switch dbtype {
				case "sqlite", "postgres":
					sqlUpdate = "UPDATE _schema_migrations SET migrated = true WHERE file = $1"
				case "mysql", "mariadb":
					sqlUpdate = "UPDATE _schema_migrations SET migrated = true WHERE file = ?"
				default:
					log.Fatalf("Migrate2: Unsupported database type: %s", dbtype)
				}
				_, err = conn.Exec(sqlUpdate, entry.Name)
				if err != nil {
					log.Fatalf("Error updating migration status for %s: %v\n", entry.Name, err)
				}

				err = PullDBSchema(conn, dbtype, schemaPath)
				if err != nil {
					log.Fatalf("Error pulling DB schema after migration %s: %v\n", entry.Name, err)
				}
				fmt.Printf("Schema successfully migrated %s\n", entry.Name)
			}
		}
		return
	}
}

type MigrateFlag struct {
	isSet bool
	value string
}

func (m *MigrateFlag) String() string {
	if !m.isSet {
		return ""
	}
	return m.value
}
func (m *MigrateFlag) Set(s string) error {
	m.isSet = true
	if s == "" {
		m.value = "true"
	} else {
		m.value = s
	}
	return nil
}
func (m *MigrateFlag) IsBoolFlag() bool {
	return true
}

func CheckTableExists(conn *sql.DB, dbtype string, rdir string) {
	var query string
	var name string

	switch dbtype {
	case "sqlite":
		query = "SELECT name FROM sqlite_master WHERE type='table' AND name='_schema_migrations'"
	case "postgres":
		query = "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename = '_schema_migrations'"
	case "mysql", "mariadb":
		query = "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '_schema_migrations'"
	default:
		log.Fatalf("Unsupported database type for table existence check: %s", dbtype)
	}

	err := conn.QueryRow(query).Scan(&name)

	if err == sql.ErrNoRows {
		sqlFile, err := os.ReadFile(fmt.Sprintf("./%s/migrations/0_init.sql", rdir))
		if err != nil {
			log.Fatalf("Error reading SQL file: %v\n", err)
		}

		_, err = conn.Exec(string(sqlFile))
		if err != nil {
			log.Fatalf("Error executing SQL to create _schema_migrations table: %v\n", err)
		}

		var sqlInsert string
		switch dbtype {
		case "sqlite", "postgres":
			sqlInsert = "INSERT INTO _schema_migrations (file, migrated) VALUES ($1, true)"
		case "mysql", "mariadb":
			sqlInsert = "INSERT INTO _schema_migrations (file, migrated) VALUES (?, true)"
		}
		_, err = conn.Exec(sqlInsert, "0_init.sql")
		if err != nil {
			log.Fatalf("Error executing SQL to insert 0_init.sql record: %v\n", err)
		}

		err = PullDBSchema(conn, dbtype, fmt.Sprintf("./%s/db.schema", rdir))
		if err != nil {
			log.Fatalf("Migrate2: Err pulling schema %v\n", err)
		}

		fmt.Println("Schema DB successfully initialized")
		return
	} else if err != nil {
		log.Fatalf("Error querying table existence: %v\n", err)
	}
}

func Conn2DB(schemaFilePath string) (*sql.DB, string, error) {
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
	default:
		return nil, "", fmt.Errorf("unsupported database type '%s' in schema '%s'", dbType, schemaFilePath)
	}
	conn, err := sql.Open(driverName, dbURL)
	if err != nil {
		return nil, "", fmt.Errorf("failed to open DB connection: %v", err)
	}
	return conn, dbType, nil
}

func PullDBSchema(conn *sql.DB, dbtype, schemaFilePath string) error {
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
	case "sqlite":
		rows, err := conn.Query("SELECT sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_schema_migrations'")
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
			modifiedStmt := strings.Replace(createStmt, "CREATE TABLE ", "table ", 1)
			formattedStmt := modifiedStmt
			if strings.Contains(formattedStmt, "(") && strings.Contains(formattedStmt, ")") {
				openParen := strings.Index(formattedStmt, "(")
				closeParen := strings.LastIndex(formattedStmt, ")")

				tableNamePart := formattedStmt[:openParen+1]
				columnsPart := formattedStmt[openParen+1 : closeParen]
				restOfStmt := formattedStmt[closeParen:]
				columnDefs := strings.Split(columnsPart, ",")
				for i, colDef := range columnDefs {
					columnDefs[i] = "  " + strings.TrimSpace(colDef)
				}
				formattedColumns := strings.Join(columnDefs, ",\n")
				formattedStmt = tableNamePart + "\n" + formattedColumns + "\n" + restOfStmt
			}
			tableSchemas = append(tableSchemas, formattedStmt)
		}
		schema = strings.Join(tableSchemas, "\n\n")
	case "postgres":
		rows, err := conn.Query(`
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

		fkRows, err := conn.Query(`
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
		rows, err := conn.Query("SHOW TABLES")
		if err != nil {
			return fmt.Errorf("error querying mysql tables: %w", err)
		}
		defer rows.Close()

		var tableNames []string
		for rows.Next() {
			var tableName string
			if err := rows.Scan(&tableName); err != nil {
				return fmt.Errorf("error scanning mysql table name: %s, %v\n", tableName, err)
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

func printTable(headers []string, data [][]string) {
	gray := lipgloss.Color("245")
	lightGray := lipgloss.Color("241")
	purple := lipgloss.Color("#FFFFFF")

	headerStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color(purple)).
		Bold(true).
		Align(lipgloss.Center)
	cellBaseStyle := lipgloss.NewStyle().Padding(0, 1)
	oddRowStyle := cellBaseStyle.Foreground(gray)
	evenRowStyle := cellBaseStyle.Foreground(lightGray)

	t := table.New().
		Border(lipgloss.NormalBorder()).
		BorderStyle(lipgloss.NewStyle().Foreground(purple)).
		Headers(headers...).
		Rows(data...).
		StyleFunc(func(row, col int) lipgloss.Style {
			switch {
			case row == table.HeaderRow:
				return headerStyle
			case row%2 == 0:
				return evenRowStyle
			default:
				return oddRowStyle
			}
		})

	fmt.Println(t.Render())
}

func flagUsed(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}
