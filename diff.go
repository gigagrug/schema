package main

import (
	"fmt"
	"strings"
)

// SchemaDiff holds all the differences between the current database and the desired schema.
type SchemaDiff struct {
	TablesToCreate []Table
	TablesToDrop   []Table
	TablesToAlter  []TableDiff
	TablesToRename []TableRename
	EnumsToAlter   []EnumDiff
}

type TableRename struct {
	OldName string
	NewName string
}

// EnumDiff tracks modifications to an Enum
type EnumDiff struct {
	Name        string
	ValuesToAdd []string
	Enum        Enum
}

// TableDiff holds the differences for a specific table.
type TableDiff struct {
	TableName         string
	DesiredTable      Table
	ColumnsToAdd      []Column
	ColumnsToDrop     []Column
	ColumnsToModify   []ColumnDiff
	ColumnsToRename   []ColumnRename
	IndexesToAdd      []Index
	IndexesToDrop     []Index
	ConstraintsToAdd  []Constraint
	ConstraintsToDrop []Constraint
}

// ColumnDiff tracks how an existing column changed
type ColumnDiff struct {
	Old Column
	New Column
}

type ColumnRename struct {
	OldName string
	NewName string
}

// HasChanges returns true if there are any column changes.
func (td *TableDiff) HasChanges() bool {
	return len(td.ColumnsToAdd) > 0 || len(td.ColumnsToDrop) > 0 ||
		len(td.ColumnsToModify) > 0 || len(td.ColumnsToRename) > 0 ||
		len(td.IndexesToAdd) > 0 || len(td.IndexesToDrop) > 0 ||
		len(td.ConstraintsToAdd) > 0 || len(td.ConstraintsToDrop) > 0
}

// DiffSchemas compares the current database state with the desired local schema.
func DiffSchemas(current, desired *Database) SchemaDiff {
	diff := SchemaDiff{}

	// Map tables for easy lookup
	currentTables := make(map[string]Table)
	for _, t := range current.Tables {
		currentTables[t.Name] = t
	}

	desiredTables := make(map[string]Table)
	for _, t := range desired.Tables {
		if isInternalTable(t.Name) {
			continue
		}
		desiredTables[t.Name] = t
	}

	// 0. Intercept Table Renames FIRST
	for name, dTable := range desiredTables {
		if dTable.OldName != "" {
			if cTable, exists := currentTables[dTable.OldName]; exists {
				diff.TablesToRename = append(diff.TablesToRename, TableRename{
					OldName: dTable.OldName,
					NewName: dTable.Name,
				})

				cTable.Name = dTable.Name

				tDiff := DiffTables(cTable, dTable)
				if tDiff.HasChanges() {
					diff.TablesToAlter = append(diff.TablesToAlter, tDiff)
				}

				delete(desiredTables, name)
				delete(currentTables, dTable.OldName)
			}
		}
	}

	// 1. Find Tables to Create and Tables to Alter (from remaining)
	for name, dTable := range desiredTables {
		if cTable, exists := currentTables[name]; !exists {
			diff.TablesToCreate = append(diff.TablesToCreate, dTable)
		} else {
			tDiff := DiffTables(cTable, dTable)
			if tDiff.HasChanges() {
				diff.TablesToAlter = append(diff.TablesToAlter, tDiff)
			}
		}
	}

	// 2. Find Tables to Drop
	for name, cTable := range currentTables {
		if _, exists := desiredTables[name]; !exists {
			// NEW: Ignore internal migration, SQLite, and Turso sync tables
			isInternal := name == "_schema_migrations" ||
				strings.HasPrefix(name, "sqlite_") ||
				strings.HasPrefix(name, "turso_cdc") ||
				strings.HasPrefix(name, "turso_sync") ||
				strings.HasPrefix(name, "libsql_")

			if !isInternal {
				diff.TablesToDrop = append(diff.TablesToDrop, cTable)
			}
		}
	}

	// --- 2.5 AUTO-DETECT TABLE RENAMES ---
	var finalCreates []Table
	var finalDrops = diff.TablesToDrop

	for _, cTable := range diff.TablesToCreate {
		renamedFromIdx := -1
		bestScore := 0.0

		for i, dTable := range finalDrops {
			matchCount := 0
			for _, dc := range cTable.Columns {
				for _, cc := range dTable.Columns {
					if dc.Name == cc.Name && typesMatch(dc.Type, cc.Type) {
						matchCount++
						break
					}
				}
			}

			maxCols := max(len(dTable.Columns), len(cTable.Columns))

			score := float64(matchCount) / float64(maxCols)
			if score > bestScore && score >= 0.60 {
				bestScore = score
				renamedFromIdx = i
			}
		}

		if renamedFromIdx != -1 {
			dTable := finalDrops[renamedFromIdx]

			diff.TablesToRename = append(diff.TablesToRename, TableRename{
				OldName: dTable.Name,
				NewName: cTable.Name,
			})

			dTable.Name = cTable.Name
			tDiff := DiffTables(dTable, cTable)
			if tDiff.HasChanges() {
				diff.TablesToAlter = append(diff.TablesToAlter, tDiff)
			}

			finalDrops = append(finalDrops[:renamedFromIdx], finalDrops[renamedFromIdx+1:]...)
		} else {
			finalCreates = append(finalCreates, cTable)
		}
	}

	diff.TablesToCreate = finalCreates
	diff.TablesToDrop = finalDrops

	// 3. Diff Enums (Postgres/MySQL)
	currentEnums := make(map[string]Enum)
	for _, e := range current.Enums {
		currentEnums[e.Name] = e
	}

	for _, dE := range desired.Enums {
		if cE, exists := currentEnums[dE.Name]; exists {
			var added []string
			cValMap := make(map[string]bool)
			for _, v := range cE.Values {
				cValMap[v] = true
			}
			for _, v := range dE.Values {
				if !cValMap[v] {
					added = append(added, v)
				}
			}
			if len(added) > 0 {
				diff.EnumsToAlter = append(diff.EnumsToAlter, EnumDiff{Name: dE.Name, ValuesToAdd: added, Enum: dE})
			}
		}
	}

	return diff
}

// constraintSignature generates a unique string for a constraint to diff them even without explicit names
func constraintSignature(c Constraint) string {
	if c.Name != "" {
		return c.Name
	}
	sig := string(c.Kind) + ":" + strings.Join(c.Columns, ",")
	switch c.Kind {
	case ForeignKey:
		sig += ":" + c.ReferenceTable + ":" + strings.Join(c.ReferenceColumns, ",")
		// FIX 2: Silent Cascade Bug (Track ON DELETE / ON UPDATE rule changes)
		if c.OnDelete != "" {
			sig += ":DEL=" + c.OnDelete
		}
		if c.OnUpdate != "" {
			sig += ":UPD=" + c.OnUpdate
		}
	case Check:
		sig += ":" + strings.ReplaceAll(strings.ToUpper(c.CheckExpression), " ", "")
	}
	return sig
}

func typesMatch(cType, dType DataType) bool {
	c := strings.ToUpper(string(cType))
	d := strings.ToUpper(string(dType))

	isIntC := c == "INTEGER" || c == "INT" || c == "INT4" || c == "SERIAL" || c == "BIGSERIAL"
	isIntD := d == "INTEGER" || d == "INT" || d == "INT4" || d == "SERIAL" || d == "BIGSERIAL"
	if isIntC && isIntD {
		return true
	}

	return c == d
}

// FIX 3: Default Value Normalization Helper
func defaultsMatch(cDef, dDef string) bool {
	c := strings.TrimSpace(cDef)
	d := strings.TrimSpace(dDef)

	if c == d {
		return true
	}

	// Strip Postgres type casts (e.g., 'project:'::text -> 'project:')
	c = strings.Split(c, "::")[0]
	d = strings.Split(d, "::")[0]

	// Strip surrounding quotes for a clean loose comparison
	c = strings.Trim(c, "'\"")
	d = strings.Trim(d, "'\"")

	return c == d
}

// DiffTables compares the columns of two tables with the same name.
func DiffTables(current, desired Table) TableDiff {
	diff := TableDiff{TableName: current.Name, DesiredTable: desired}

	currentCols := make(map[string]Column)
	for _, c := range current.Columns {
		currentCols[c.Name] = c
	}

	desiredCols := make(map[string]Column)
	for _, c := range desired.Columns {
		desiredCols[c.Name] = c
	}

	// 1. Intercept Columns to Rename FIRST
	for name, dCol := range desiredCols {
		if dCol.OldName != "" {
			if cCol, exists := currentCols[dCol.OldName]; exists {
				diff.ColumnsToRename = append(diff.ColumnsToRename, ColumnRename{
					OldName: dCol.OldName,
					NewName: dCol.Name,
				})

				// Uses the new defaultsMatch normalizer
				if !typesMatch(cCol.Type, dCol.Type) || cCol.IsNullable != dCol.IsNullable || !defaultsMatch(cCol.DefaultValue, dCol.DefaultValue) {
					diff.ColumnsToModify = append(diff.ColumnsToModify, ColumnDiff{Old: cCol, New: dCol})
				}

				delete(desiredCols, name)
				delete(currentCols, dCol.OldName)
			}
		}
	}

	// 2. Find Columns to Add & Modify (from remaining)
	for name, dCol := range desiredCols {
		if cCol, exists := currentCols[name]; !exists {
			diff.ColumnsToAdd = append(diff.ColumnsToAdd, dCol)
		} else {
			// Uses the new defaultsMatch normalizer
			if !typesMatch(cCol.Type, dCol.Type) || cCol.IsNullable != dCol.IsNullable || !defaultsMatch(cCol.DefaultValue, dCol.DefaultValue) {
				diff.ColumnsToModify = append(diff.ColumnsToModify, ColumnDiff{Old: cCol, New: dCol})
			}
		}
	}

	// 3. Find Columns to Drop (from remaining)
	for name, cCol := range currentCols {
		if _, exists := desiredCols[name]; !exists {
			diff.ColumnsToDrop = append(diff.ColumnsToDrop, cCol)
		}
	}

	// --- 3.5 AUTO-DETECT COLUMN RENAMES ---
	var finalColAdds []Column
	var finalColDrops = diff.ColumnsToDrop

	for _, addCol := range diff.ColumnsToAdd {
		renamedFromIdx := -1
		bestScore := 0

		for i, dropCol := range finalColDrops {
			score := 0

			if typesMatch(addCol.Type, dropCol.Type) {
				score += 2
			}
			if addCol.IsNullable == dropCol.IsNullable {
				score += 1
			}
			// Uses the new defaultsMatch normalizer
			if defaultsMatch(addCol.DefaultValue, dropCol.DefaultValue) {
				score += 1
			}

			if len(diff.ColumnsToAdd) == 1 && len(diff.ColumnsToDrop) == 1 {
				score += 10
			}

			if score > bestScore && score >= 2 {
				bestScore = score
				renamedFromIdx = i
			}
		}

		if renamedFromIdx != -1 {
			dropCol := finalColDrops[renamedFromIdx]

			diff.ColumnsToRename = append(diff.ColumnsToRename, ColumnRename{
				OldName: dropCol.Name,
				NewName: addCol.Name,
			})

			// Uses the new defaultsMatch normalizer
			if !typesMatch(dropCol.Type, addCol.Type) || dropCol.IsNullable != addCol.IsNullable || !defaultsMatch(dropCol.DefaultValue, addCol.DefaultValue) {
				diff.ColumnsToModify = append(diff.ColumnsToModify, ColumnDiff{Old: dropCol, New: addCol})
			}

			finalColDrops = append(finalColDrops[:renamedFromIdx], finalColDrops[renamedFromIdx+1:]...)
		} else {
			// FIX 4: "NOT NULL Without Default" Trap Warning
			if !addCol.IsNullable && addCol.DefaultValue == "" && !addCol.IsAutoIncrement {
				fmt.Printf("\033[33mWarning: You are adding a NOT NULL column '%s' to '%s' without a default value. This migration will fail if the table already has existing rows!\033[0m\n", addCol.Name, diff.TableName)
			}
			finalColAdds = append(finalColAdds, addCol)
		}
	}

	diff.ColumnsToAdd = finalColAdds
	diff.ColumnsToDrop = finalColDrops

	// 4. Diff Indexes
	currentIdxs := make(map[string]Index)
	for _, idx := range current.Indexes {
		currentIdxs[idx.Name] = idx
	}
	desiredIdxs := make(map[string]Index)
	for _, idx := range desired.Indexes {
		desiredIdxs[idx.Name] = idx
	}

	for name, dIdx := range desiredIdxs {
		if cIdx, exists := currentIdxs[name]; !exists {
			diff.IndexesToAdd = append(diff.IndexesToAdd, dIdx)
		} else {
			// FIX 1: Silent Index Bug (Compare Columns and IsUnique flag)
			colsMatch := len(dIdx.Columns) == len(cIdx.Columns)
			if colsMatch {
				for i, c := range dIdx.Columns {
					if c != cIdx.Columns[i] {
						colsMatch = false
						break
					}
				}
			}

			// If they kept the name, but changed the underlying structure, drop and recreate!
			if dIdx.IsUnique != cIdx.IsUnique || !colsMatch {
				diff.IndexesToDrop = append(diff.IndexesToDrop, cIdx)
				diff.IndexesToAdd = append(diff.IndexesToAdd, dIdx)
			}
		}
	}
	for name, cIdx := range currentIdxs {
		if _, exists := desiredIdxs[name]; !exists {
			diff.IndexesToDrop = append(diff.IndexesToDrop, cIdx)
		}
	}

	// 5. Diff Constraints
	currentCons := make(map[string]Constraint)
	for _, c := range current.Constraints {
		currentCons[constraintSignature(c)] = c
	}
	desiredCons := make(map[string]Constraint)
	for _, c := range desired.Constraints {
		desiredCons[constraintSignature(c)] = c
	}

	for sig, dC := range desiredCons {
		if _, exists := currentCons[sig]; !exists {
			if dC.Kind != PrimaryKey {
				diff.ConstraintsToAdd = append(diff.ConstraintsToAdd, dC)
			}
		}
	}
	for sig, cC := range currentCons {
		if _, exists := desiredCons[sig]; !exists {
			if cC.Kind != PrimaryKey {
				diff.ConstraintsToDrop = append(diff.ConstraintsToDrop, cC)
			}
		}
	}

	return diff
}

// GenerateMigrationSQL takes the SchemaDiff and database type, returning the SQL statements needed to apply the changes.
func GenerateMigrationSQL(diff SchemaDiff, dbType string) string {
	var statements []string

	for _, eDiff := range diff.EnumsToAlter {
		switch dbType {
		case "postgres":
			for _, v := range eDiff.ValuesToAdd {
				statements = append(statements, fmt.Sprintf("ALTER TYPE %s ADD VALUE '%s';", eDiff.Name, v))
			}
		case "mysql", "mariadb":
			parts := strings.SplitN(eDiff.Name, "_", 2)
			if len(parts) == 2 {
				tableName, colName := parts[0], parts[1]
				vals := []string{}
				for _, v := range eDiff.Enum.Values {
					vals = append(vals, fmt.Sprintf("'%s'", v))
				}
				enumStr := fmt.Sprintf("ENUM(%s)", strings.Join(vals, ", "))
				statements = append(statements, fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s %s;", tableName, colName, enumStr))
			}
		}
	}

	for _, rename := range diff.TablesToRename {
		statements = append(statements, fmt.Sprintf("ALTER TABLE %s RENAME TO %s;", rename.OldName, rename.NewName))
	}

	for _, t := range diff.TablesToDrop {
		statements = append(statements, fmt.Sprintf("DROP TABLE %s;", t.Name))
	}

	for _, t := range diff.TablesToCreate {
		statements = append(statements, generateCreateTableSQL(t, dbType))
	}

	for _, tDiff := range diff.TablesToAlter {
		isSQLite := dbType == "sqlite" || dbType == "libsql" || dbType == "turso" || dbType == "tursosync"

		if isSQLite && (len(tDiff.ColumnsToModify) > 0 || len(tDiff.ConstraintsToAdd) > 0 || len(tDiff.ConstraintsToDrop) > 0) {
			statements = append(statements, generateSQLiteTableRebuild(tDiff))

			for _, idx := range tDiff.DesiredTable.Indexes {
				uniq := ""
				if idx.IsUnique {
					uniq = "UNIQUE "
				}
				statements = append(statements, fmt.Sprintf("CREATE %sINDEX %s ON %s (%s);", uniq, idx.Name, tDiff.TableName, strings.Join(idx.Columns, ", ")))
			}
			continue
		}

		for _, rename := range tDiff.ColumnsToRename {
			statements = append(statements, fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s;", tDiff.TableName, rename.OldName, rename.NewName))
		}

		for _, c := range tDiff.ConstraintsToDrop {
			if c.Name != "" {
				if dbType == "mysql" || dbType == "mariadb" {
					switch c.Kind {
					case ForeignKey:
						statements = append(statements, fmt.Sprintf("ALTER TABLE %s DROP FOREIGN KEY %s;", tDiff.TableName, c.Name))
					case Unique:
						statements = append(statements, fmt.Sprintf("ALTER TABLE %s DROP INDEX %s;", tDiff.TableName, c.Name))
					case Check:
						statements = append(statements, fmt.Sprintf("ALTER TABLE %s DROP CHECK %s;", tDiff.TableName, c.Name))
					}
				} else {
					statements = append(statements, fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s;", tDiff.TableName, c.Name))
				}
			}
		}

		for _, col := range tDiff.ColumnsToAdd {
			colDef := formatColumnDefinition(col, dbType)
			statements = append(statements, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s;", tDiff.TableName, colDef))
		}

		for _, col := range tDiff.ColumnsToDrop {
			statements = append(statements, fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s;", tDiff.TableName, col.Name))
		}

		for _, colDiff := range tDiff.ColumnsToModify {
			statements = append(statements, generateColumnModifySQL(tDiff.TableName, colDiff, dbType))
		}

		for _, c := range tDiff.ConstraintsToAdd {
			constraintDef := ""
			switch c.Kind {
			case ForeignKey:
				constraintDef = fmt.Sprintf("FOREIGN KEY (%s) REFERENCES %s(%s)", strings.Join(c.Columns, ", "), c.ReferenceTable, strings.Join(c.ReferenceColumns, ", "))
				if c.OnDelete != "" && c.OnDelete != "NO ACTION" {
					constraintDef += " ON DELETE " + c.OnDelete
				}
				if c.OnUpdate != "" && c.OnUpdate != "NO ACTION" {
					constraintDef += " ON UPDATE " + c.OnUpdate
				}
			case Unique:
				constraintDef = fmt.Sprintf("UNIQUE (%s)", strings.Join(c.Columns, ", "))
			case Check:
				constraintDef = fmt.Sprintf("CHECK (%s)", c.CheckExpression)
			}

			if constraintDef != "" {
				if c.Name != "" {
					statements = append(statements, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s;", tDiff.TableName, c.Name, constraintDef))
				} else {
					statements = append(statements, fmt.Sprintf("ALTER TABLE %s ADD %s;", tDiff.TableName, constraintDef))
				}
			}
		}

		for _, idx := range tDiff.IndexesToDrop {
			if dbType == "mysql" || dbType == "mariadb" {
				statements = append(statements, fmt.Sprintf("DROP INDEX %s ON %s;", idx.Name, tDiff.TableName))
			} else {
				statements = append(statements, fmt.Sprintf("DROP INDEX %s;", idx.Name))
			}
		}

		for _, idx := range tDiff.IndexesToAdd {
			uniq := ""
			if idx.IsUnique {
				uniq = "UNIQUE "
			}
			statements = append(statements, fmt.Sprintf("CREATE %sINDEX %s ON %s (%s);", uniq, idx.Name, tDiff.TableName, strings.Join(idx.Columns, ", ")))
		}
	}

	for _, t := range diff.TablesToCreate {
		for _, idx := range t.Indexes {
			uniq := ""
			if idx.IsUnique {
				uniq = "UNIQUE "
			}
			statements = append(statements, fmt.Sprintf("CREATE %sINDEX %s ON %s (%s);", uniq, idx.Name, t.Name, strings.Join(idx.Columns, ", ")))
		}
	}

	return strings.Join(statements, "\n\n")
}

func generateCreateTableSQL(t Table, dbType string) string {
	var lines []string
	inlinePKs := make(map[string]bool)

	for _, col := range t.Columns {
		isInlinePK := col.IsAutoIncrement && (dbType == "sqlite" || dbType == "libsql" || dbType == "turso" || dbType == "tursosync")
		if isInlinePK {
			inlinePKs[col.Name] = true
		}
		lines = append(lines, "  "+formatColumnDefinition(col, dbType))
	}

	for _, c := range t.Constraints {
		switch c.Kind {
		case PrimaryKey:
			if len(c.Columns) == 1 && inlinePKs[c.Columns[0]] {
				continue
			}
			lines = append(lines, fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(c.Columns, ", ")))
		case ForeignKey:
			fkLine := fmt.Sprintf("  FOREIGN KEY (%s) REFERENCES %s(%s)", strings.Join(c.Columns, ", "), c.ReferenceTable, strings.Join(c.ReferenceColumns, ", "))
			if c.OnDelete != "" && c.OnDelete != "NO ACTION" {
				fkLine += fmt.Sprintf(" ON DELETE %s", c.OnDelete)
			}
			if c.OnUpdate != "" && c.OnUpdate != "NO ACTION" {
				fkLine += fmt.Sprintf(" ON UPDATE %s", c.OnUpdate)
			}
			lines = append(lines, fkLine)
		case Unique:
			lines = append(lines, fmt.Sprintf("  UNIQUE (%s)", strings.Join(c.Columns, ", ")))
		case Check:
			if c.Name != "" {
				lines = append(lines, fmt.Sprintf("  CONSTRAINT %s CHECK (%s)", c.Name, c.CheckExpression))
			} else {
				lines = append(lines, fmt.Sprintf("  CHECK (%s)", c.CheckExpression))
			}
		}
	}

	return fmt.Sprintf("CREATE TABLE %s (\n%s\n);", t.Name, strings.Join(lines, ",\n"))
}

func formatColumnDefinition(col Column, dbType string) string {
	colType := string(col.Type)
	defVal := col.DefaultValue

	if col.IsAutoIncrement {
		switch dbType {
		case "sqlite", "libsql", "turso", "tursosync":
			if colType == "INTEGER" || colType == "INT" {
				colType = "INTEGER PRIMARY KEY AUTOINCREMENT"
			}
		case "postgres":
			switch colType {
			case "INTEGER", "INT", "INT4":
				colType = "SERIAL"
			case "BIGINT", "INT8":
				colType = "BIGSERIAL"
			}
		case "mysql", "mariadb":
			colType += " AUTO_INCREMENT"
		}
	}

	line := fmt.Sprintf("%s %s", col.Name, colType)

	if !col.IsNullable {
		line += " NOT NULL"
	}

	if defVal != "" && !strings.HasPrefix(defVal, "nextval(") {
		val := strings.Split(defVal, "::")[0]
		line += fmt.Sprintf(" DEFAULT %s", val)
	}

	return line
}

func generateColumnModifySQL(tableName string, diff ColumnDiff, dbType string) string {
	var stmts []string
	col := diff.New

	switch dbType {
	case "postgres":
		if diff.Old.Type != col.Type {
			stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s USING %s::%s;", tableName, col.Name, col.Type, col.Name, col.Type))
		}
		if diff.Old.IsNullable != col.IsNullable {
			if col.IsNullable {
				stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL;", tableName, col.Name))
			} else {
				stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL;", tableName, col.Name))
			}
		}
		if !defaultsMatch(diff.Old.DefaultValue, col.DefaultValue) {
			if col.DefaultValue == "" {
				stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT;", tableName, col.Name))
			} else {
				stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s;", tableName, col.Name, col.DefaultValue))
			}
		}
	case "mysql", "mariadb":
		colDef := formatColumnDefinition(col, dbType)
		stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s;", tableName, colDef))
	}

	return strings.Join(stmts, "\n")
}

func generateSQLiteTableRebuild(tDiff TableDiff) string {
	var stmts []string
	tempTableName := tDiff.TableName + "_new_tmp"

	tempTable := tDiff.DesiredTable
	tempTable.Name = tempTableName
	stmts = append(stmts, generateCreateTableSQL(tempTable, "sqlite"))

	var selectCols []string
	var insertCols []string
	for _, newCol := range tDiff.DesiredTable.Columns {
		isAdded := false
		for _, addCol := range tDiff.ColumnsToAdd {
			if addCol.Name == newCol.Name {
				isAdded = true
				break
			}
		}

		if !isAdded {
			insertCols = append(insertCols, newCol.Name)

			selectName := newCol.Name
			for _, rename := range tDiff.ColumnsToRename {
				if rename.NewName == newCol.Name {
					selectName = rename.OldName
					break
				}
			}
			selectCols = append(selectCols, selectName)
		}
	}

	if len(insertCols) > 0 {
		insString := strings.Join(insertCols, ", ")
		selString := strings.Join(selectCols, ", ")
		stmts = append(stmts, fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s;", tempTableName, insString, selString, tDiff.TableName))
	}

	stmts = append(stmts, fmt.Sprintf("DROP TABLE %s;", tDiff.TableName))
	stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s RENAME TO %s;", tempTableName, tDiff.TableName))

	return strings.Join(stmts, "\n")
}

// isInternalTable checks if a table is a system/replication table that should be ignored
func isInternalTable(name string) bool {
	return name == "_schema_migrations" ||
		strings.HasPrefix(name, "sqlite_") ||
		strings.HasPrefix(name, "turso_cdc") ||
		strings.HasPrefix(name, "turso_sync") ||
		strings.HasPrefix(name, "libsql_")
}
