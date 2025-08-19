---
outline: deep
---

# Inserts

## Insert Dummy Data
### Step 1
Doesn't save in _schema_migrations table if not in migrations dir so they can be reused
```shell
schema -create="insertdata" -dir="inserts"
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
### Step 3
```shell
schema -sql="0_insertdata.sql" -dir="inserts"
```
