package main

import (
	"os"
	"regexp"
	"strings"
)

// ParseSchemaFile reads a db.schema file and converts it into a Database struct.
func ParseSchemaFile(filePath string) (*Database, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	db := &Database{}
	strContent := string(content)

	// 1. Parse Enums (Updated regex to handle spaces before the closing parenthesis)
	enumRe := regexp.MustCompile(`(?m)^enum\s+([a-zA-Z0-9_]+)\s*\(([\s\S]*?)\n\s*\)`)
	enumMatches := enumRe.FindAllStringSubmatch(strContent, -1)
	for _, m := range enumMatches {
		enumName := m[1]
		valuesStr := m[2]

		var values []string
		for v := range strings.SplitSeq(valuesStr, ",") {
			val := strings.TrimSpace(v)
			val = strings.Trim(val, "'\"\n ") // clean up quotes and spaces
			if val != "" {
				values = append(values, val)
			}
		}
		db.Enums = append(db.Enums, Enum{Name: enumName, Values: values})
	}

	// 2. Parse Tables (Updated regex to handle spaces/carriage returns before closing parenthesis)
	tableRe := regexp.MustCompile(`(?mi)^table\s+([a-zA-Z0-9_]+)(?:\s+FROM\s+([a-zA-Z0-9_]+))?\s*\(([\s\S]*?)\n\s*\)`)
	tableMatches := tableRe.FindAllStringSubmatch(strContent, -1)

	for _, m := range tableMatches {
		tableName := m[1]
		oldName := m[2]
		body := m[3]

		table := Table{
			Name:    tableName,
			OldName: oldName,
		}

		// Split table body by lines
		lines := strings.SplitSeq(body, "\n")
		for line := range lines {
			line = strings.TrimSpace(line)
			line = strings.TrimSuffix(line, ",") // Remove trailing commas
			if line == "" {
				continue
			}

			upperLine := strings.ToUpper(line)

			// Handle Table Indexes
			if strings.HasPrefix(upperLine, "INDEX ") {
				table.Indexes = append(table.Indexes, parseIndex(line))
				continue
			}

			// Handle Table Checks/Constraints
			if strings.HasPrefix(upperLine, "CONSTRAINT ") || strings.HasPrefix(upperLine, "CHECK ") {
				table.Constraints = append(table.Constraints, parseCheck(line))
				continue
			}

			// Handle Columns
			col := parseColumn(line)
			table.Columns = append(table.Columns, col)

			// Extract inline constraints (PK, Unique, FK) into the table's constraint list
			if strings.Contains(upperLine, "PRIMARY KEY") {
				table.Constraints = append(table.Constraints, Constraint{
					Kind:    PrimaryKey,
					Columns: []string{col.Name},
				})
			}
			if strings.Contains(upperLine, "UNIQUE") {
				table.Constraints = append(table.Constraints, Constraint{
					Kind:    Unique,
					Columns: []string{col.Name},
				})
			}
			if strings.Contains(upperLine, "REFERENCES") {
				table.Constraints = append(table.Constraints, parseInlineFK(line, col.Name))
			}
		}
		db.Tables = append(db.Tables, table)
	}

	return db, nil
}

// --- Parsing Helpers ---

func parseColumn(line string) Column {
	parts := strings.Fields(line)
	if len(parts) < 2 {
		return Column{Name: parts[0]}
	}

	col := Column{
		Name:       parts[0],
		Type:       DataType(parts[1]),
		IsNullable: true,
	}

	upperLine := strings.ToUpper(line)

	if strings.Contains(upperLine, "NOT NULL") {
		col.IsNullable = false
	}

	if strings.Contains(upperLine, "AUTOINCREMENT") || strings.Contains(upperLine, "AUTO_INCREMENT") || strings.Contains(upperLine, "SERIAL") {
		col.IsAutoIncrement = true
	}

	// Extract Default Value
	defRe := regexp.MustCompile(`(?i)DEFAULT\s+([^ ]+|'[^']+')`)
	if m := defRe.FindStringSubmatch(line); len(m) > 1 {
		col.DefaultValue = m[1]
	}

	// Extract Rename (e.g., FROM username)
	renameRe := regexp.MustCompile(`(?i)FROM\s+([a-zA-Z0-9_]+)`)
	if m := renameRe.FindStringSubmatch(line); len(m) > 1 {
		col.OldName = m[1]
	}

	return col

}

func parseIndex(line string) Index {
	re := regexp.MustCompile(`(?i)INDEX\s+([a-zA-Z0-9_]+)\s*\((.*?)\)`)
	m := re.FindStringSubmatch(line)
	if len(m) < 3 {
		return Index{}
	}

	cols := strings.Split(m[2], ",")
	for i := range cols {
		cols[i] = strings.TrimSpace(cols[i])
	}

	return Index{
		Name:     m[1],
		Columns:  cols,
		IsUnique: false,
	}
}

func parseCheck(line string) Constraint {
	c := Constraint{Kind: Check}

	if strings.HasPrefix(strings.ToUpper(line), "CONSTRAINT") {
		re := regexp.MustCompile(`(?i)CONSTRAINT\s+([a-zA-Z0-9_]+)\s+CHECK\s*\((.*?)\)`)
		if m := re.FindStringSubmatch(line); len(m) > 2 {
			c.Name = m[1]
			c.CheckExpression = m[2]
		}
	} else {
		re := regexp.MustCompile(`(?i)CHECK\s*\((.*?)\)`)
		if m := re.FindStringSubmatch(line); len(m) > 1 {
			c.CheckExpression = m[1]
		}
	}
	return c
}

func parseInlineFK(line, colName string) Constraint {
	c := Constraint{
		Kind:    ForeignKey,
		Columns: []string{colName},
	}

	re := regexp.MustCompile(`(?i)REFERENCES\s+([a-zA-Z0-9_]+)\s*\((.*?)\)`)
	if m := re.FindStringSubmatch(line); len(m) > 2 {
		c.ReferenceTable = m[1]
		c.ReferenceColumns = []string{strings.TrimSpace(m[2])}
	}

	delRe := regexp.MustCompile(`(?i)ON\s+DELETE\s+(CASCADE|SET\s+NULL|SET\s+DEFAULT|RESTRICT|NO\s+ACTION)`)
	if m := delRe.FindStringSubmatch(line); len(m) > 1 {
		c.OnDelete = strings.ToUpper(m[1])
	}

	updRe := regexp.MustCompile(`(?i)ON\s+UPDATE\s+(CASCADE|SET\s+NULL|SET\s+DEFAULT|RESTRICT|NO\s+ACTION)`)
	if m := updRe.FindStringSubmatch(line); len(m) > 1 {
		c.OnUpdate = strings.ToUpper(m[1])
	}

	return c
}
