# Schema
A language agnostic CLI tool for handling database migrations | SQLite, PostgreSQL, MySQL, MariaDB

## Installation
Install/upgrade latest version
```shell
curl -sSfL https://raw.githubusercontent.com/gigagrug/schema/main/install.sh | sh -s
```
Install specific version 
```shell
curl -sSfL https://raw.githubusercontent.com/gigagrug/schema/main/install.sh | sh -s 0.2.0
```

## Get Started
### Step 1
Init project (default: db=sqlite url=dev.db) 
```shell
schema -i
```
Init project using another db (sqlite, postrges, mysql, mariadb)
```shell
schema -i -db="postgres" -url="postgresql://postgres:postgres@localhost:5432/postgres"
```

### Step 2
Create a SQL file
```shell
schema -create="initschema"
```

### Step 3
Go to ./schema/migrations/1_initschema.sql (This SQL is for sqlite)
```shell
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

### Step 4
Migrates all the sql files not migrated 
```shell
schema -migrate
``` 
