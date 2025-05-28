package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/joho/godotenv"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	initFS := flag.Bool("initfs", false, "init schema files")
	initDB := flag.Bool("initdb", false, "init DB migrations")
	db := flag.String("db", "sqlite", "add db: sqlite, postgres, mysql, mariadb")
	dbURL := flag.String("dburl", "dev.db", "add dburl")
	create := flag.String("create", "", "create sql file")
	migrate := flag.String("migrate", "", "migrate database")
	flag.Parse()

	if *initFS {
		if _, err := os.Stat("./schema/"); os.IsNotExist(err) {
			err := os.MkdirAll("./schema/migrations/", 0755)
			if err != nil {
				log.Fatal(err)
			}
			schemaFile, err := os.Create("./schema/db.schema")
			if err != nil {
				fmt.Println("Error creating file:", err)
				return
			}
			defer schemaFile.Close()

			fileContent := fmt.Sprintf("db = \"%s\"\ndbURL = env(\"DB_URL\")", *db)
			_, err = schemaFile.WriteString(fileContent)
			if err != nil {
				fmt.Println("Error writing to file:", err)
				return
			}

			file, err := os.Create("./schema/migrations/0_init.sql")
			if err != nil {
				fmt.Println("Error creating file:", err)
				return
			}
			defer file.Close()

			sqlTable := "CREATE TABLE IF NOT EXISTS _schema_migrations (\n  id INTEGER PRIMARY KEY AUTOINCREMENT, \n  file VARCHAR(255) UNIQUE,\n  migrated BOOLEAN DEFAULT false\n);"
			if *db == "sqlite" {
				sqlTable = "CREATE TABLE IF NOT EXISTS _schema_migrations (\n  id INTEGER PRIMARY KEY AUTOINCREMENT, \n  file VARCHAR(255) UNIQUE,\n  migrated BOOLEAN DEFAULT false\n);"
			} else if *db == "postgres" {
				sqlTable = "CREATE TABLE IF NOT EXISTS _schema_migrations (\n  id SERIAL PRIMARY KEY, \n  file VARCHAR(255) UNIQUE,\n  migrated BOOLEAN DEFAULT false\n);"
			} else if *db == "mysql" || *db == "mariadb" {
				sqlTable = "CREATE TABLE IF NOT EXISTS _schema_migrations (\n  id INT PRIMARY KEY AUTO_INCREMENT, \n  file VARCHAR(255) UNIQUE,\n  migrated BOOLEAN DEFAULT false\n);"
			}

			_, err = file.WriteString(sqlTable)
			if err != nil {
				fmt.Println("Error writing to file:", err)
				return
			}
		}

		if _, err := os.Stat("./.env"); os.IsNotExist(err) {
			envFile, err := os.Create("./.env")
			if err != nil {
				fmt.Println("Error creating file:", err)
				return
			}
			defer envFile.Close()

			schemaContent := fmt.Sprintf(`DB_URL="%s"`, *dbURL)
			_, err = envFile.WriteString(schemaContent)
			if err != nil {
				fmt.Println("Error writing to file:", err)
				return
			}
		}

		fmt.Println("Schema FS successfully initialized")

		if !*initDB {
			return
		}
	}

	if *initDB {
		conn, dbtype, err := connectToDBFromSchema("./schema/db.schema")
		if err != nil {
			fmt.Println(err)
			return
		}
		defer conn.Close()

		sqlFile, err := os.ReadFile("./schema/migrations/0_init.sql")
		if err != nil {
			log.Fatalf("Error reading SQL file: %v\n", err)
			return
		}

		_, err = conn.Exec(string(sqlFile))
		if err != nil {
			log.Fatalf("Error executing SQL: %v\n", err)
			return
		}

		var sqlInsert string
		if dbtype == "sqlite" {
			sqlInsert = "INSERT INTO _schema_migrations (file, migrated) VALUES ($1, true)"
		} else if dbtype == "postgres" {
			sqlInsert = "INSERT INTO _schema_migrations (file, migrated) VALUES ($1, true)"
		} else if dbtype == "mysql" || *db == "mariadb" {
			sqlInsert = "INSERT INTO _schema_migrations (file, migrated) VALUES (?, true)"
		}
		_, err = conn.Exec(sqlInsert, "0_init.sql")
		if err != nil {
			log.Fatalf("Error executing SQL: %v\n", err)
			return
		}

		fmt.Println("Schema DB successfully initialized")
		return
	}

	if *create != "" {
		conn, dbtype, err := connectToDBFromSchema("./schema/db.schema")
		if err != nil {
			fmt.Println(err)
			return
		}
		defer conn.Close()

		dirPath := "./schema/migrations/"

		entries, err := os.ReadDir(dirPath)
		if err != nil {
			log.Fatalf("Failed to read directory '%s': %v", dirPath, err)
			return
		}

		fileCount := 0
		for _, entry := range entries {
			if !entry.IsDir() {
				fileCount++
			}
		}
		fileName := fmt.Sprintf("%d_%s.sql", fileCount, *create)
		schemaFile, err := os.Create("./schema/migrations/" + fileName)
		if err != nil {
			fmt.Println("Error creating file:", err)
			return
		}
		defer schemaFile.Close()

		var sqlInsert string
		if dbtype == "sqlite" {
			sqlInsert = "INSERT INTO _schema_migrations (file) VALUES ($1)"
		} else if dbtype == "postgres" {
			sqlInsert = "INSERT INTO _schema_migrations (file) VALUES ($1)"
		} else if dbtype == "mysql" || *db == "mariadb" {
			sqlInsert = "INSERT INTO _schema_migrations (file) VALUES (?)"
		}
		_, err = conn.Exec(sqlInsert, fileName)
		if err != nil {
			log.Fatalf("Error executing SQL: %v\n", err)
			return
		}

		fmt.Printf("Schema successfully created sql file %s\n", fileName)
		return
	}

	if *migrate != "" {
		fileP := fmt.Sprintf("./schema/migrations/%s", *migrate)
		sqlFile, err := os.ReadFile(fileP)
		if err != nil {
			log.Fatalf("Error reading SQL file: %v\n", err)
			return
		}

		conn, dbtype, err := connectToDBFromSchema("./schema/db.schema")
		if err != nil {
			fmt.Println(err)
			return
		}
		defer conn.Close()
		_, err = conn.Exec(string(sqlFile))
		if err != nil {
			log.Fatalf("Error executing SQL: %v\n", err)
			return
		}

		var sqlUpdate string
		if dbtype == "sqlite" {
			sqlUpdate = "UPDATE _schema_migrations SET migrated = true WHERE file = $1"
		} else if dbtype == "postgres" {
			sqlUpdate = "UPDATE _schema_migrations SET migrated = true WHERE file = $1"
		} else if dbtype == "mysql" || *db == "mariadb" {
			sqlUpdate = "UPDATE _schema_migrations SET migrated = true WHERE file = ?"
		}
		_, err = conn.Exec(sqlUpdate, *migrate)
		if err != nil {
			log.Fatalf("Error executing SQL: %v\n", err)
			return
		}

		fmt.Printf("Schema successfully migrated %s\n", *migrate)
		return

	} else {

		conn, dbtype, err := connectToDBFromSchema("./schema/db.schema")
		if err != nil {
			fmt.Println(err)
			return
		}
		defer conn.Close()

		rows, err := conn.Query(`SELECT file FROM _schema_migrations WHERE migrated = false ORDER BY id ASC`)
		if err != nil {
			log.Fatalf("Error executing SQL: %v\n", err)
			return
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
				log.Fatalf("Error scanning row: %v\n", err)
				return
			}
			files = append(files, Files{Name: file})
		}

		if len(files) == 0 {
			fmt.Println("No pending migrations found.")
			return
		}

		for _, entry := range files {
			fileP := fmt.Sprintf("./schema/migrations/%s", entry.Name)
			sqlFile, err := os.ReadFile(fileP)
			if err != nil {
				log.Fatalf("Error reading SQL file: %v\n", err)
				return
			}

			_, err = conn.Exec(string(sqlFile))
			if err != nil {
				log.Fatalf("Error executing SQL: %v\n", err)
				return
			}

			var sqlUpdate string
			if dbtype == "sqlite" {
				sqlUpdate = "UPDATE _schema_migrations SET migrated = true WHERE file = $1"
			} else if dbtype == "postgres" {
				sqlUpdate = "UPDATE _schema_migrations SET migrated = true WHERE file = $1"
			} else if dbtype == "mysql" || *db == "mariadb" {
				sqlUpdate = "UPDATE _schema_migrations SET migrated = true WHERE file = ?"
			}
			_, err = conn.Exec(sqlUpdate, entry.Name)
			if err != nil {
				log.Fatalf("Error executing SQL: %v\n", err)
				return
			}

			fmt.Printf("Schema successfully migrated %s\n", entry.Name)
		}
	}
}

func connectToDBFromSchema(schemaFilePath string) (*sql.DB, string, error) {
	err := godotenv.Load()
	if err != nil {
		return nil, "", fmt.Errorf("error loading .env file: %w", err)
	}

	file, err := os.Open(schemaFilePath)
	if err != nil {
		return nil, "", fmt.Errorf("failed to open schema file '%s': %w", schemaFilePath, err)
	}
	defer file.Close()

	var dbType string
	var dbURL string
	foundDbType := false
	lineNumber := 0
	dbTypePrefix := "db ="
	dbURLPrefix := "dbURL ="
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

	var conn *sql.DB

	if dbType == "sqlite" {
		conn, err = sql.Open("sqlite3", dbURL)
	} else if dbType == "postgres" {
		conn, err = sql.Open("pgx", dbURL)
	} else if dbType == "mysql" || dbType == "mariadb" {
		conn, err = sql.Open("mysql", dbURL)
	} else {
		return nil, "", fmt.Errorf("unsupported database type '%s' in schema '%s'", dbType, schemaFilePath)
	}

	if err != nil {
		return nil, "", fmt.Errorf("failed to connect to %s: %w", dbType, err)
	}
	return conn, dbType, nil
}
