---
outline: deep
---

# SQLite: Alter Table
Offical docs: https://www.sqlite.org/lang_altertable.html

## Basic
### Syntax
Add column
```sql
ALTER TABLE table_name ADD column_name datatype constraint;
```

Drop column
```sql
ALTER TABLE table_name DROP column_name;
```

Drop table
```sql
DROP TABLE IF EXISTS table_name;
```

### Example
```sql
ALTER TABLE users ADD date_of_birth DATE;
```
