---
outline: deep
---

# Get Started
The default database is sqlite

## Initialize Schema
### Step 1
Init project (default: db=sqlite url=./schema/dev.db) 
```shell
schema i
```
Init project using another db and url (sqlite, libsql, postrges, mysql, mariadb)
```shell
schema i -db="postgres" -url="postgresql://postgres:postgres@localhost:5432/postgres"
```
Init project with different root directory<br>
For sqlite the url is automatically set as whatever else the root directory is set as
```shell
schema i -rdir="schema2"
```
### Step 2
Nessesary if using existing database<br>
Schema is found in rdir/db.schema
```shell
schema pull
```
