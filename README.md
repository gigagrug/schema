# Schema
A CLI tool for working with the database | SQLite, libSQL, PostgreSQL, MySQL, MariaDB

![schema](./docs/assets/schema.png)

## Installation
Install/upgrade latest version
```shell
curl -sSfL https://raw.githubusercontent.com/gigagrug/schema/main/install.sh | sh -s
```
Install specific version 
```shell
curl -sSfL https://raw.githubusercontent.com/gigagrug/schema/main/install.sh | sh -s 0.1.0
```

## Get Started
### Step 1
Init project (default: db=sqlite url=./schema/dev.db) 
```shell
schema -i
```
Init project using another db and url
```shell
schema -i -db="postgres" -url="postgresql://postgres:postgres@localhost:5432/postgres"
```
Init project with different root directory
```shell
schema -i -rdir="schema2"
```
### Step 2
Nessesary if using existing database
```shell
schema -pull
```

## Migrations
### Step 1
Create a SQL file
```shell
schema -create="initschema"
```
### Step 2
Go to ./schema/migrations/1_initschema.sql (This SQL is for sqlite)
```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    email TEXT NOT NULL UNIQUE,
    password TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```
### Step 3
Migrates all the sql files not migrated 
```shell
schema -migrate
```
Migrates specific sql file
```shell
schema -migrate="1_initschema.sql"
```

## Insert Dummy Data
### Step 1
Doesn't save in _schema_migrations table if not in migrations dir so they can be reused
```shell
schema -create="insertdata" -dir="inserts"
```
### Step 2
Insert based on the SQL schema above. 
```sql
WITH RECURSIVE generate_users AS (
  SELECT ABS(RANDOM() % 10000) AS random_number, 1 AS row_number
  UNION ALL
  SELECT ABS(RANDOM() % 10000), row_number + 1
  FROM generate_users
  WHERE row_number < 5
)
INSERT INTO users (username, email, password)
SELECT 
  'user_' || random_number, 
  'user_' || random_number || '@example.com', 
  'password'
FROM generate_users;

WITH RECURSIVE user_list AS (
  SELECT id, ROW_NUMBER() OVER () AS rn
  FROM users
  ORDER BY RANDOM()
  LIMIT 5
),
post_insert AS (
  SELECT
    'Post #' || ABS(RANDOM() % 10000) AS title,
    'This is a post about random topic #' || ABS(RANDOM() % 10000) AS content,
    id AS user_id
  FROM user_list
)
INSERT INTO posts (user_id, title, content)
SELECT user_id, title, content FROM post_insert;
```
### Step 3
```shell
schema -sql="0_insertdata.sql" -dir="inserts"
```

## Select query and prints table in console
```shell
schema -sql="SELECT * FROM users"
```
![table](./docs/assets/table.png)

## TUI SQL Studio
```shell
schema -studio
```
![studio](./docs/assets/studio.png)

## Flags
`v`: prints your version and latest version <br>
`i`: initializes project<br>
`pull`: pulls database schema <br>
`migrate`: migrates all file in migrations dir <br>
`studio`: tui sql studio<br>
`migrate="[select file under migrations/]"` <br>
`sql="[select .sql file or input the sql query directly]"` <br>
`db="[sqlite, libsql, postgres, mysql, mariadb]"` (default sqlite) <br>
`url="[database url]"` (default ./schema/dev.db) <br>
`create="[file name]"`: create file the number and .sql is added for you "1_filename.sql" <br>
`dir="[choose/create directory under schema/]"` (default migration) <br>
`rdir="[choose/create root directory]"` (default schema)
