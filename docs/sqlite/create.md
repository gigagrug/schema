---
outline: deep
---

# SQLite: Create Table
Offical docs: https://sqlite.org/lang_createtable.html

## Basic
### Syntax
By default datatypes are not enforced you can add 'STRICT' to have the datatype be enforced.
```sql
CREATE TABLE table_name (
    column1 datatype constraint, 
    column2 datatype constraint constraint constraint, 
    column3 datatype constraint constraint,
)STRICT;
```

### Example
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

## Reference
### Datatypes
Offical docs: https://www.sqlite.org/datatype3.html

INT<br>
INTEGER<br>
TINYINT<br>
SMALLINT<br>
MEDIUMINT<br>
BIGINT<br>
UNSIGNED BIG INT<br>
INT2<br>
INT8<br>

CHARACTER(20)<br>
VARCHAR(255)<br>
VARYING CHARACTER(255)<br>
NCHAR(55)<br>
NATIVE CHARACTER(70)<br>
NVARCHAR(100)<br>
TEXT<br>
CLOB<br>

BLOB

REAL<br>
DOUBLE<br>
DOUBLE PRECISION<br>
FLOAT<br>

NUMERIC<br>
DECIMAL(10,5)<br>
BOOLEAN<br>
DATE<br>
DATETIME<br>

### Constraints
PRIMARY KEY<br>
UNIQUE<br>
NOT NULL<br>
CHECK<br>
DEFAULT<br>
FOREIGN KEY<br>
AUTOINCREMENT<br>
REFERENCES<br> 
CASCADE<br>
ON UPDATE<br>
ON DELETE<br>
