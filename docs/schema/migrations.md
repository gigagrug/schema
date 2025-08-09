---
outline: deep
---

# Migrations
## Step 1
Create a SQL file
```shell
schema -create="initschema"
```
## Step 2
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
## Step 3
Migrates all the sql files not migrated 
```shell
schema -migrate
```
Migrates specific sql file
```shell
schema -migrate="1_initschema"
```
