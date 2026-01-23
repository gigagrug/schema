package main

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
)

type Dialect struct{ Type, TableExists, CreateInit, Insert, Update, Delete, SelectStatus, ListTables, ListCols string }

func GetDialect(dbType string) Dialect {
	switch dbType {
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

type (
	ConstraintKind string
	DataType       string
)

const (
	PrimaryKey ConstraintKind = "PRIMARY_KEY"
	ForeignKey ConstraintKind = "FOREIGN_KEY"
	Unique     ConstraintKind = "UNIQUE"
	Check      ConstraintKind = "CHECK"
)

type Database struct {
	Name   string
	Tables []Table
	Enums  []Enum
}

type Table struct {
	Name        string
	Columns     []Column
	Constraints []Constraint
	Indexes     []Index
}

type Column struct {
	Name            string
	Type            DataType
	IsNullable      bool
	DefaultValue    string
	IsAutoIncrement bool
}

type Constraint struct {
	Name             string
	Kind             ConstraintKind
	Columns          []string
	ReferenceTable   string
	ReferenceColumns []string
	CheckExpression  string
	OnDelete         string
	OnUpdate         string
}

type Index struct {
	Name     string
	Columns  []string
	IsUnique bool
}

type Enum struct {
	Name   string
	Values []string
}

// --- Introspection Interface & Logic ---

type schemaDriver interface {
	Name(ctx context.Context) (string, error)
	Tables(ctx context.Context) ([]string, error)
	Columns(ctx context.Context, table string) ([]Column, error)
	Constraints(ctx context.Context, table string) ([]Constraint, error)
	Indexes(ctx context.Context, table string) ([]Index, error)
	Enums(ctx context.Context) ([]Enum, error)
}

func InspectSchema(ctx context.Context, db *sql.DB, dbType string) (*Database, error) {
	var drv schemaDriver
	switch dbType {
	case "sqlite", "libsql", "turso":
		drv = &sqliteDriver{db}
	case "postgres":
		drv = &postgresDriver{db}
	case "mysql", "mariadb":
		drv = &mysqlDriver{db}
	default:
		return nil, fmt.Errorf("unsupported database type: %s", dbType)
	}

	name, err := drv.Name(ctx)
	if err != nil {
		return nil, err
	}

	tableNames, err := drv.Tables(ctx)
	if err != nil {
		return nil, err
	}

	var tables []Table
	for _, tName := range tableNames {
		cols, err := drv.Columns(ctx, tName)
		if err != nil {
			return nil, err
		}

		constrs, err := drv.Constraints(ctx, tName)
		if err != nil {
			return nil, err
		}

		idxs, err := drv.Indexes(ctx, tName)
		if err != nil {
			return nil, err
		}

		tables = append(tables, Table{
			Name:        tName,
			Columns:     cols,
			Constraints: constrs,
			Indexes:     idxs,
		})
	}

	enums, err := drv.Enums(ctx)
	if err != nil {
		return nil, err
	}

	return &Database{Name: name, Tables: tables, Enums: enums}, nil
}

type sqliteDriver struct{ db *sql.DB }

func (s *sqliteDriver) Name(ctx context.Context) (string, error) { return "sqlite", nil }

func (s *sqliteDriver) Tables(ctx context.Context) ([]string, error) {
	tables, err := queryStrings(ctx, s.db, "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name != '_schema_migrations'")
	if err != nil {
		return nil, err
	}
	for i, j := 0, len(tables)-1; i < j; i, j = i+1, j-1 {
		tables[i], tables[j] = tables[j], tables[i]
	}
	return tables, nil
}

func (s *sqliteDriver) Enums(ctx context.Context) ([]Enum, error) { return nil, nil }

func (s *sqliteDriver) Columns(ctx context.Context, table string) ([]Column, error) {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(\"%s\")", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []Column
	var autoIncMap = make(map[string]bool)
	var sqlStr string
	if err := s.db.QueryRowContext(ctx, "SELECT sql FROM sqlite_master WHERE type='table' AND name = ?", table).Scan(&sqlStr); err == nil {
		for line := range strings.SplitSeq(sqlStr, "\n") {
			if strings.Contains(strings.ToUpper(line), "AUTOINCREMENT") {
				parts := strings.Fields(strings.TrimSpace(line))
				if len(parts) > 0 {
					autoIncMap[strings.Trim(parts[0], "\"`[]")] = true
				}
			}
		}
	}

	for rows.Next() {
		var cid, notnull, pk int
		var name, dtype string
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &dtype, &notnull, &dflt, &pk); err != nil {
			return nil, err
		}
		cols = append(cols, Column{
			Name:            name,
			Type:            DataType(strings.ToUpper(dtype)),
			IsNullable:      notnull == 0,
			DefaultValue:    dflt.String,
			IsAutoIncrement: autoIncMap[name],
		})
	}
	return cols, nil
}

func (s *sqliteDriver) Constraints(ctx context.Context, table string) ([]Constraint, error) {
	var cs []Constraint
	// PKs
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(\"%s\")", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pkCols []string
	for rows.Next() {
		var cid, notnull, pk int
		var name, dtype string
		var dflt sql.NullString
		rows.Scan(&cid, &name, &dtype, &notnull, &dflt, &pk)
		if pk > 0 {
			pkCols = append(pkCols, name)
		}
	}
	if len(pkCols) > 0 {
		cs = append(cs, Constraint{Kind: PrimaryKey, Columns: pkCols})
	}
	rows.Close()

	// FKs
	rows, err = s.db.QueryContext(ctx, fmt.Sprintf("PRAGMA foreign_key_list(\"%s\")", table))
	if err == nil {
		defer rows.Close()
		type fkInfo struct {
			table, onUp, onDel string
			cols, refCols      []string
		}
		fkMap := make(map[int]*fkInfo)
		var order []int
		for rows.Next() {
			var id, seq int
			var tbl, from, to, up, del, match string
			rows.Scan(&id, &seq, &tbl, &from, &to, &up, &del, &match)
			if _, ok := fkMap[id]; !ok {
				fkMap[id] = &fkInfo{table: tbl, onUp: up, onDel: del}
				order = append(order, id)
			}
			fkMap[id].cols = append(fkMap[id].cols, from)
			fkMap[id].refCols = append(fkMap[id].refCols, to)
		}
		for _, id := range order {
			info := fkMap[id]
			cs = append(cs, Constraint{
				Kind: ForeignKey, ReferenceTable: info.table, Columns: info.cols,
				ReferenceColumns: info.refCols, OnUpdate: info.onUp, OnDelete: info.onDel,
			})
		}
	}

	// CHECKs
	var sqlStr string
	if err := s.db.QueryRowContext(ctx, "SELECT sql FROM sqlite_master WHERE type='table' AND name = ?", table).Scan(&sqlStr); err == nil {
		re := regexp.MustCompile(`(?i)\bCHECK\s*\((.*)\)`)
		for line := range strings.SplitSeq(sqlStr, "\n") {
			if matches := re.FindStringSubmatch(line); len(matches) > 1 {
				expr := matches[1]
				if before, ok := strings.CutSuffix(expr, "),"); ok {
					expr = before
				} else {
					expr = strings.TrimSuffix(expr, ")")
				}
				cs = append(cs, Constraint{Kind: Check, CheckExpression: strings.TrimSpace(expr)})
			}
		}
	}
	return cs, nil
}

func (s *sqliteDriver) Indexes(ctx context.Context, table string) ([]Index, error) {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf("PRAGMA index_list(\"%s\")", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var idxs []Index
	for rows.Next() {
		var seq, unique, partial int
		var name, origin string
		rows.Scan(&seq, &name, &unique, &origin, &partial)
		if origin == "pk" || origin == "u" {
			continue
		}
		iRows, _ := s.db.QueryContext(ctx, fmt.Sprintf("PRAGMA index_info(\"%s\")", name))
		var cols []string
		for iRows.Next() {
			var seqno, cid int
			var cName string
			iRows.Scan(&seqno, &cid, &cName)
			cols = append(cols, cName)
		}
		iRows.Close()
		idxs = append(idxs, Index{Name: name, Columns: cols, IsUnique: unique == 1})
	}
	return idxs, nil
}

type postgresDriver struct{ db *sql.DB }

func (p *postgresDriver) Name(ctx context.Context) (string, error) {
	var name string
	err := p.db.QueryRowContext(ctx, "SELECT current_database()").Scan(&name)
	return name, err
}

func (p *postgresDriver) Tables(ctx context.Context) ([]string, error) {
	return queryStrings(ctx, p.db, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE' ORDER BY table_name")
}

func (p *postgresDriver) Enums(ctx context.Context) ([]Enum, error) {
	rows, err := p.db.QueryContext(ctx, "SELECT t.typname, e.enumlabel FROM pg_type t JOIN pg_enum e ON t.oid = e.enumtypid JOIN pg_namespace n ON n.oid = t.typnamespace WHERE n.nspname = 'public' ORDER BY t.typname, e.enumsortorder")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	enumMap := make(map[string][]string)
	var names []string
	for rows.Next() {
		var n, v string
		rows.Scan(&n, &v)
		if _, ok := enumMap[n]; !ok {
			names = append(names, n)
		}
		enumMap[n] = append(enumMap[n], v)
	}
	var enums []Enum
	for _, n := range names {
		enums = append(enums, Enum{Name: n, Values: enumMap[n]})
	}
	return enums, nil
}

func (p *postgresDriver) Columns(ctx context.Context, table string) ([]Column, error) {
	q := `SELECT column_name, data_type, udt_name, character_maximum_length, numeric_precision, numeric_scale, is_nullable, column_default 
	      FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 ORDER BY ordinal_position`
	rows, err := p.db.QueryContext(ctx, q, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cols []Column
	for rows.Next() {
		var name, dtype, udt, isNull string
		var charMax, numPrec, numScale sql.NullInt64
		var def sql.NullString
		rows.Scan(&name, &dtype, &udt, &charMax, &numPrec, &numScale, &isNull, &def)
		cols = append(cols, Column{
			Name: name, IsNullable: isNull == "YES", DefaultValue: def.String,
			Type: DataType(formatPgType(dtype, udt, charMax, numPrec, numScale)),
		})
	}
	return cols, nil
}

func (p *postgresDriver) Constraints(ctx context.Context, table string) ([]Constraint, error) {
	var cs []Constraint
	// PK & Unique
	q := `SELECT tc.constraint_name, tc.constraint_type, kcu.column_name 
	      FROM information_schema.table_constraints tc 
	      JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name 
	      WHERE tc.table_schema='public' AND tc.table_name=$1 AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE') 
	      ORDER BY tc.constraint_name, kcu.ordinal_position`
	rows, err := p.db.QueryContext(ctx, q, table)
	if err == nil {
		defer rows.Close()
		cmap := make(map[string]*Constraint)
		var order []string
		for rows.Next() {
			var cname, ctype, col string
			rows.Scan(&cname, &ctype, &col)
			if _, ok := cmap[cname]; !ok {
				k := Unique
				if ctype == "PRIMARY KEY" {
					k = PrimaryKey
				}
				cmap[cname] = &Constraint{Name: cname, Kind: k}
				order = append(order, cname)
			}
			cmap[cname].Columns = append(cmap[cname].Columns, col)
		}
		for _, n := range order {
			cs = append(cs, *cmap[n])
		}
	}
	// FKs
	qFK := `SELECT con.conname, ref_rel.relname, con.conkey, con.confkey, con.confdeltype, con.confupdtype
			FROM pg_constraint con JOIN pg_class src ON src.oid=con.conrelid JOIN pg_class ref_rel ON ref_rel.oid=con.confrelid
			WHERE src.relname=$1 AND con.contype='f'`
	rows, err = p.db.QueryContext(ctx, qFK, table)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var name, refT string
			var k1, k2 []byte
			var dt, ut []byte // raw chars
			rows.Scan(&name, &refT, &k1, &k2, &dt, &ut)
			cols := resolvePgCols(ctx, p.db, table, parseInt16Array(string(k1)))
			refCols := resolvePgCols(ctx, p.db, refT, parseInt16Array(string(k2)))
			cs = append(cs, Constraint{
				Name: name, Kind: ForeignKey, ReferenceTable: refT, Columns: cols, ReferenceColumns: refCols,
				OnDelete: parsePgRule(dt), OnUpdate: parsePgRule(ut),
			})
		}
	}
	// Checks
	qChk := `SELECT con.conname, pg_get_constraintdef(con.oid) FROM pg_constraint con JOIN pg_class r ON r.oid=con.conrelid WHERE r.relname=$1 AND con.contype='c'`
	rows, err = p.db.QueryContext(ctx, qChk, table)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var name, def string
			rows.Scan(&name, &def)
			if strings.HasPrefix(def, "CHECK (") {
				def = def[7 : len(def)-1]
			}
			cs = append(cs, Constraint{Name: name, Kind: Check, CheckExpression: def})
		}
	}
	return cs, nil
}

func (p *postgresDriver) Indexes(ctx context.Context, table string) ([]Index, error) {
	q := `SELECT i.relname, array_to_string(array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)), ', '), ix.indisunique
		  FROM pg_class t JOIN pg_index ix ON t.oid = ix.indrelid JOIN pg_class i ON i.oid = ix.indexrelid
		  JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
		  LEFT JOIN pg_constraint c ON c.conindid = i.oid
		  WHERE t.relname=$1 AND c.conindid IS NULL GROUP BY i.relname, ix.indisunique, ix.indkey`
	rows, err := p.db.QueryContext(ctx, q, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var idxs []Index
	for rows.Next() {
		var name, colStr string
		var uniq bool
		rows.Scan(&name, &colStr, &uniq)
		idxs = append(idxs, Index{Name: name, IsUnique: uniq, Columns: strings.Split(colStr, ", ")})
	}
	return idxs, nil
}

type mysqlDriver struct{ db *sql.DB }

func (m *mysqlDriver) Name(ctx context.Context) (string, error) {
	var n string
	err := m.db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&n)
	return n, err
}
func (m *mysqlDriver) Tables(ctx context.Context) ([]string, error) {
	return queryStrings(ctx, m.db, "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE' AND table_name != '_schema_migrations'")
}
func (m *mysqlDriver) Enums(ctx context.Context) ([]Enum, error) {
	q := `SELECT table_name, column_name, column_type FROM information_schema.columns WHERE table_schema = DATABASE() AND data_type = 'enum'`
	rows, err := m.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var enums []Enum
	seen := make(map[string]bool)
	for rows.Next() {
		var tn, cn, ctype string
		rows.Scan(&tn, &cn, &ctype)
		eName := tn + "_" + cn
		if cn == "status" && strings.HasSuffix(tn, "s") {
			eName = tn[:len(tn)-1] + "_" + cn
		}
		if !seen[eName] {
			vals := strings.Split(strings.TrimSuffix(strings.TrimPrefix(ctype, "enum("), ")"), ",")
			for i := range vals {
				vals[i] = strings.Trim(strings.TrimSpace(vals[i]), "'")
			}
			enums = append(enums, Enum{Name: eName, Values: vals})
			seen[eName] = true
		}
	}
	return enums, nil
}
func (m *mysqlDriver) Columns(ctx context.Context, table string) ([]Column, error) {
	q := `SELECT column_name, data_type, column_type, is_nullable, column_default, extra FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? ORDER BY ordinal_position`
	rows, err := m.db.QueryContext(ctx, q, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cols []Column
	for rows.Next() {
		var name, dt, ct, isNull, extra string
		var def sql.NullString
		rows.Scan(&name, &dt, &ct, &isNull, &def, &extra)
		ft := strings.ToUpper(dt)
		if dt == "enum" {
			ft = table + "_" + name
			if name == "status" && strings.HasSuffix(table, "s") {
				ft = table[:len(table)-1] + "_" + name
			}
		} else if strings.Contains(ct, "tinyint(1)") {
			ft = "BOOLEAN"
		} else if dt == "int" {
			ft = "INTEGER"
		}
		cols = append(cols, Column{Name: name, Type: DataType(ft), IsNullable: isNull == "YES", DefaultValue: def.String, IsAutoIncrement: strings.Contains(extra, "auto_increment")})
	}
	return cols, nil
}
func (m *mysqlDriver) Constraints(ctx context.Context, table string) ([]Constraint, error) {
	var cs []Constraint
	// PK
	qPK := `SELECT column_name FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu USING(constraint_name, table_schema) WHERE table_schema = DATABASE() AND table_name = ? AND constraint_type = 'PRIMARY KEY' ORDER BY kcu.ordinal_position`
	if pkCols, err := queryStrings(ctx, m.db, qPK, table); err == nil && len(pkCols) > 0 {
		cs = append(cs, Constraint{Kind: PrimaryKey, Columns: pkCols})
	}
	// FK
	qFK := `SELECT constraint_name, column_name, referenced_table_name, referenced_column_name, update_rule, delete_rule FROM information_schema.referential_constraints JOIN information_schema.key_column_usage USING(constraint_name, constraint_schema) WHERE table_schema = DATABASE() AND table_name = ? ORDER BY constraint_name, ordinal_position`
	rows, err := m.db.QueryContext(ctx, qFK, table)
	if err == nil {
		defer rows.Close()
		fkMap := make(map[string]*Constraint)
		var order []string
		for rows.Next() {
			var cn, col, rt, rc, ur, dr string
			rows.Scan(&cn, &col, &rt, &rc, &ur, &dr)
			if _, ok := fkMap[cn]; !ok {
				fkMap[cn] = &Constraint{Name: cn, Kind: ForeignKey, ReferenceTable: rt, OnUpdate: ur, OnDelete: dr}
				order = append(order, cn)
			}
			fkMap[cn].Columns = append(fkMap[cn].Columns, col)
			fkMap[cn].ReferenceColumns = append(fkMap[cn].ReferenceColumns, rc)
		}
		for _, n := range order {
			cs = append(cs, *fkMap[n])
		}
	}
	// Unique
	qUQ := `SELECT constraint_name, column_name FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu USING(constraint_name, table_schema) WHERE table_schema = DATABASE() AND tc.table_name = ? AND constraint_type = 'UNIQUE' ORDER BY constraint_name, ordinal_position`
	rows, err = m.db.QueryContext(ctx, qUQ, table)
	if err == nil {
		defer rows.Close()
		uMap := make(map[string][]string)
		var order []string
		for rows.Next() {
			var n, c string
			rows.Scan(&n, &c)
			if _, ok := uMap[n]; !ok {
				order = append(order, n)
			}
			uMap[n] = append(uMap[n], c)
		}
		for _, n := range order {
			cs = append(cs, Constraint{Name: n, Kind: Unique, Columns: uMap[n]})
		}
	}
	// Check (Parsing SHOW CREATE TABLE)
	var ct, createSQL string
	if err := m.db.QueryRowContext(ctx, fmt.Sprintf("SHOW CREATE TABLE %s", table)).Scan(&ct, &createSQL); err == nil {
		re := regexp.MustCompile(`CONSTRAINT\s+["']?(\w+)["']?\s+CHECK\s*\((.*)\)`)
		re2 := regexp.MustCompile(`\bCHECK\s*\((.*)\)`)
		for line := range strings.SplitSeq(createSQL, "\n") {
			line = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(line), ","))
			if m := re.FindStringSubmatch(line); len(m) > 2 {
				cs = append(cs, Constraint{Name: m[1], Kind: Check, CheckExpression: stripParens(m[2])})
			} else if m := re2.FindStringSubmatch(line); len(m) > 1 && !strings.HasPrefix(line, "CONSTRAINT") {
				cs = append(cs, Constraint{Kind: Check, CheckExpression: stripParens(m[1])})
			}
		}
	}
	return cs, nil
}
func (m *mysqlDriver) Indexes(ctx context.Context, table string) ([]Index, error) {
	rows, err := m.db.QueryContext(ctx, fmt.Sprintf("SHOW INDEX FROM %s", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	idxMap := make(map[string]*Index)
	var order []string
	cols, _ := rows.Columns()
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	for rows.Next() {
		rows.Scan(ptrs...)
		row := make(map[string]string)
		for i, c := range cols {
			if v, ok := vals[i].([]byte); ok {
				row[c] = string(v)
			} else {
				row[c] = fmt.Sprintf("%v", vals[i])
			}
		}
		if row["Key_name"] == "PRIMARY" {
			continue
		}
		name := row["Key_name"]
		if _, ok := idxMap[name]; !ok {
			idxMap[name] = &Index{Name: name, IsUnique: row["Non_unique"] == "0", Columns: make([]string, 10)} // simple alloc
			order = append(order, name)
		}
		var seq int
		fmt.Sscanf(row["Seq_in_index"], "%d", &seq)
		if seq > 0 {
			if seq > len(idxMap[name].Columns) { // grow if needed
				newC := make([]string, seq+5)
				copy(newC, idxMap[name].Columns)
				idxMap[name].Columns = newC
			}
			idxMap[name].Columns[seq-1] = row["Column_name"]
		}
	}
	var idxs []Index
	for _, n := range order {
		i := idxMap[n]
		var clean []string
		for _, c := range i.Columns {
			if c != "" {
				clean = append(clean, c)
			}
		}
		i.Columns = clean
		idxs = append(idxs, *i)
	}
	return idxs, nil
}

// --- Helpers ---

func queryStrings(ctx context.Context, db *sql.DB, query string, args ...any) ([]string, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var res []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		res = append(res, s)
	}
	return res, nil
}

func stripParens(s string) string {
	s = strings.TrimSpace(s)
	for strings.HasPrefix(s, "(") && strings.HasSuffix(s, ")") {
		s = s[1 : len(s)-1]
	}
	return s
}

func formatPgType(dt, udt string, max, prec, scale sql.NullInt64) string {
	switch dt {
	case "USER-DEFINED":
		return udt
	case "character varying":
		if max.Valid {
			return fmt.Sprintf("VARCHAR(%d)", max.Int64)
		}
		return "VARCHAR"
	case "character":
		if max.Valid {
			return fmt.Sprintf("CHAR(%d)", max.Int64)
		}
		return "CHAR"
	case "numeric":
		if prec.Valid && scale.Valid {
			return fmt.Sprintf("NUMERIC(%d,%d)", prec.Int64, scale.Int64)
		}
		return "NUMERIC"
	default:
		return strings.ToUpper(dt)
	}
}

func resolvePgCols(ctx context.Context, db *sql.DB, table string, nums []int16) []string {
	if len(nums) == 0 {
		return nil
	}
	args := make([]any, len(nums)+1)
	args[0] = table
	placeholders := make([]string, len(nums))
	for i, n := range nums {
		args[i+1] = n
		placeholders[i] = fmt.Sprintf("$%d", i+2)
	}
	q := fmt.Sprintf("SELECT attname, attnum FROM pg_attribute JOIN pg_class ON pg_class.oid=pg_attribute.attrelid WHERE relname=$1 AND attnum IN (%s)", strings.Join(placeholders, ","))
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil
	}
	defer rows.Close()
	m := make(map[int16]string)
	for rows.Next() {
		var name string
		var num int16
		rows.Scan(&name, &num)
		m[num] = name
	}
	var cols []string
	for _, n := range nums {
		cols = append(cols, m[n])
	}
	return cols
}

func parseInt16Array(s string) []int16 {
	s = strings.Trim(s, "{}")
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	var res []int16
	for _, p := range parts {
		var i int16
		fmt.Sscanf(p, "%d", &i)
		res = append(res, i)
	}
	return res
}

func parsePgRule(c []byte) string {
	if len(c) == 0 {
		return ""
	}
	switch c[0] {
	case 'c':
		return "CASCADE"
	case 'n':
		return "SET NULL"
	case 'd':
		return "SET DEFAULT"
	case 'r':
		return "RESTRICT"
	case 'a':
		return "NO ACTION"
	}
	return ""
}
