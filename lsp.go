package main

import (
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/tliron/commonlog"
	_ "github.com/tliron/commonlog/simple"
	"github.com/tliron/glsp"
	protocol "github.com/tliron/glsp/protocol_3_16"
)

const lspName = "Schema"

var (
	handler       protocol.Handler
	documentStore = make(map[string]string)
	mutex         sync.RWMutex
	lspLog        = commonlog.GetLogger("schema.lsp")
)

func initialize(context *glsp.Context, params *protocol.InitializeParams) (any, error) {
	capabilities := handler.CreateServerCapabilities()
	capabilities.CompletionProvider = &protocol.CompletionOptions{
		TriggerCharacters: []string{",", ".", " "},
	}
	capabilities.TextDocumentSync = protocol.TextDocumentSyncOptions{
		Change: ptr(protocol.TextDocumentSyncKindFull),
		Save:   true,
	}
	capabilities.DocumentFormattingProvider = true
	return protocol.InitializeResult{
		Capabilities: capabilities,
		ServerInfo: &protocol.InitializeResultServerInfo{
			Name:    lspName,
			Version: &version,
		},
	}, nil
}

func initialized(context *glsp.Context, params *protocol.InitializedParams) error {
	return nil
}

func shutdown(context *glsp.Context) error {
	protocol.SetTraceValue(protocol.TraceValueOff)
	return nil
}

func setTrace(context *glsp.Context, params *protocol.SetTraceParams) error {
	protocol.SetTraceValue(params.Value)
	return nil
}

func textDocumentDidOpen(context *glsp.Context, params *protocol.DidOpenTextDocumentParams) error {
	lspLog.Infof("Document opened: %s", params.TextDocument.URI)
	mutex.Lock()
	defer mutex.Unlock()
	documentStore[params.TextDocument.URI] = params.TextDocument.Text
	return nil
}

func textDocumentDidChange(context *glsp.Context, params *protocol.DidChangeTextDocumentParams) error {
	lspLog.Infof("Document changed: %s", params.TextDocument.URI)
	if len(params.ContentChanges) == 0 {
		lspLog.Warning("DidChange notification received with no content changes.")
		return nil
	}

	change, ok := params.ContentChanges[0].(protocol.TextDocumentContentChangeEventWhole)
	if !ok {
		lspLog.Errorf("ERROR: Could not assert change event to protocol.TextDocumentContentChangeEventWhole. Actual type is %T", params.ContentChanges[0])
		return nil
	}

	lspLog.Info("Storing new content from DidChange.")
	mutex.Lock()
	defer mutex.Unlock()
	documentStore[params.TextDocument.URI] = change.Text

	return nil
}

func textDocumentDidSave(context *glsp.Context, params *protocol.DidSaveTextDocumentParams) error {
	uri := params.TextDocument.URI
	lspLog.Infof("Document saved: %s", uri)
	mutex.RLock()
	content, ok := documentStore[uri]
	mutex.RUnlock()
	if !ok {
		lspLog.Warningf("DidSave notification for a document not in the store: %s", uri)
		return nil
	}
	lspLog.Infof("Triggering diagnostics/schema update for %s...", content)
	return nil
}

func isolateCurrentStatement(content string, offset int) string {
	start := strings.LastIndex(content[:offset], ";")
	if start == -1 {
		start = 0
	} else {
		start++
	}
	end := strings.Index(content[offset:], ";")
	if end == -1 {
		end = len(content)
	} else {
		end += offset
	}
	return strings.TrimSpace(content[start:end])
}

func splitOnTopCommas(s string) []string {
	var result []string
	parenLevel := 0
	lastSplit := 0

	for i, char := range s {
		switch char {
		case '(':
			parenLevel++
		case ')':
			parenLevel--
		case ',':
			if parenLevel == 0 {
				result = append(result, strings.TrimSpace(s[lastSplit:i]))
				lastSplit = i + 1
			}
		}
	}

	result = append(result, strings.TrimSpace(s[lastSplit:]))
	return result
}
func formatSql(content string) (string, error) {
	var statements []string
	var currentStatement strings.Builder
	lines := strings.SplitSeq(content, "\n")

	for line := range lines {
		trimmedLine := strings.TrimSpace(line)
		if strings.HasPrefix(trimmedLine, "--") {
			if currentStatement.Len() > 0 {
				statements = append(statements, strings.TrimSpace(currentStatement.String()))
				currentStatement.Reset()
			}
			statements = append(statements, line)
		} else {
			currentStatement.WriteString(line)
			currentStatement.WriteString("\n")
			if strings.HasSuffix(trimmedLine, ";") {
				statements = append(statements, strings.TrimSpace(currentStatement.String()))
				currentStatement.Reset()
			}
		}
	}

	if currentStatement.Len() > 0 {
		statements = append(statements, strings.TrimSpace(currentStatement.String()))
	}

	var formattedStatements []string
	for _, stmt := range statements {
		trimmedStmt := strings.TrimSpace(stmt)
		if trimmedStmt == "" {
			continue
		}

		if strings.HasPrefix(trimmedStmt, "--") {
			formattedStatements = append(formattedStatements, stmt)
			continue
		}

		stmtToFormat := strings.TrimRight(trimmedStmt, ";")
		stmtToFormat = strings.TrimSpace(stmtToFormat)

		var formatted string
		var err error
		upperStmt := strings.ToUpper(stmtToFormat)
		if strings.HasPrefix(upperStmt, "CREATE") {
			formatted, err = formatCreateTable(stmtToFormat)
		} else if strings.HasPrefix(upperStmt, "INSERT") {
			formatted, err = formatInsert(stmtToFormat)
		} else if strings.HasPrefix(upperStmt, "WITH") {
			formatted = stmtToFormat
			err = nil
		} else {
			formatted, err = formatQuery(stmtToFormat)
		}
		if err != nil {
			formatted = stmtToFormat
		}
		formattedStatements = append(formattedStatements, formatted+";")
	}
	return strings.Join(formattedStatements, "\n\n"), nil
}

func formatQuery(content string) (string, error) {
	reWhitespace := regexp.MustCompile(`\s+`)
	formattedSql := reWhitespace.ReplaceAllString(content, " ")

	reOperators := regexp.MustCompile(`\s*([=<>!]+)\s*`)
	formattedSql = reOperators.ReplaceAllString(formattedSql, " $1 ")

	reCommas := regexp.MustCompile(`\s*,\s*`)
	formattedSql = reCommas.ReplaceAllString(formattedSql, ", ")

	keywords := []string{
		"FROM", "WHERE", "GROUP BY", "ORDER BY", "LIMIT",
		"JOIN", "LEFT JOIN", "RIGHT JOIN", "INNER JOIN", "FULL OUTER JOIN",
		"UPDATE", "SET", "INSERT INTO", "VALUES",
	}

	reKeywords := regexp.MustCompile(fmt.Sprintf(`(?i)\s\b(%s)\b`, strings.Join(keywords, "|")))
	formattedSql = reKeywords.ReplaceAllString(formattedSql, "\n$1")
	return formattedSql, nil
}

func formatCreateTable(content string) (string, error) {
	formattedStmt := content
	openParen := strings.Index(formattedStmt, "(")
	closeParen := strings.LastIndex(formattedStmt, ")")

	if openParen == -1 || closeParen <= openParen {
		return formattedStmt, nil
	}
	tableNameDeclaration := formattedStmt[:openParen]
	reWhitespace := regexp.MustCompile(`\s+`)
	normalizedDeclaration := reWhitespace.ReplaceAllString(tableNameDeclaration, " ")
	trimmedDeclaration := strings.TrimSpace(normalizedDeclaration)
	finalTableNamePart := trimmedDeclaration + " ("
	columnsPart := formattedStmt[openParen+1 : closeParen]
	restOfStmt := formattedStmt[closeParen:]

	columnDefs := splitOnTopCommas(columnsPart)

	for i, colDef := range columnDefs {
		trimmed := strings.TrimSpace(colDef)
		normalized := reWhitespace.ReplaceAllString(trimmed, " ")
		columnDefs[i] = "\t" + normalized
	}
	formattedColumns := strings.Join(columnDefs, ",\n")
	finalStmt := finalTableNamePart + "\n" + formattedColumns + "\n" + restOfStmt
	return finalStmt, nil
}

func formatInsert(content string) (string, error) {
	upperContent := strings.ToUpper(content)
	valuesIndex := strings.Index(upperContent, "VALUES")
	if valuesIndex == -1 {
		return content, fmt.Errorf("no VALUES clause found in INSERT statement")
	}

	insertClause := strings.TrimSpace(content[:valuesIndex])
	parenIndex := strings.Index(insertClause, "(")

	if parenIndex > -1 {
		tablePart := strings.TrimSpace(insertClause[:parenIndex])
		columnPart := insertClause[parenIndex:]
		insertClause = fmt.Sprintf("%s\n\t%s", tablePart, columnPart)
	}

	valuesPart := strings.TrimSpace(content[valuesIndex+len("VALUES"):])
	valueTuples := splitOnTopCommas(valuesPart)

	for i, tuple := range valueTuples {
		valueTuples[i] = "\t" + strings.TrimSpace(tuple)
	}

	formattedValues := strings.Join(valueTuples, ",\n")
	return fmt.Sprintf("%s\nVALUES\n%s", insertClause, formattedValues), nil
}

func textDocumentFormatting(context *glsp.Context, params *protocol.DocumentFormattingParams) ([]protocol.TextEdit, error) {
	mutex.RLock()
	content, ok := documentStore[params.TextDocument.URI]
	mutex.RUnlock()

	if !ok {
		return nil, nil
	}

	formattedSql, err := formatSql(content)
	if err != nil {
		lspLog.Warningf("Could not format document with formatter: %v", err)
		return nil, nil
	}

	lines := strings.Split(content, "\n")
	endLine := uint32(len(lines) - 1)
	endChar := uint32(len(lines[endLine]))

	edits := []protocol.TextEdit{
		{
			Range: protocol.Range{
				Start: protocol.Position{Line: 0, Character: 0},
				End:   protocol.Position{Line: endLine, Character: endChar},
			},
			NewText: formattedSql,
		},
	}
	return edits, nil
}

func extractTableName(statement string) []string {
	statement = strings.ToUpper(statement)
	var re *regexp.Regexp
	if strings.HasPrefix(statement, "SELECT") || strings.HasPrefix(statement, "DELETE FROM") {
		re = regexp.MustCompile(`FROM\s+([^\s,]+)`)
	} else if strings.HasPrefix(statement, "UPDATE") {
		re = regexp.MustCompile(`UPDATE\s+([^\s]+)`)
	} else if strings.HasPrefix(statement, "INSERT INTO") {
		re = regexp.MustCompile(`INSERT\s+INTO\s+([^\s(]+)`)
	} else if strings.HasPrefix(statement, "ALTER TABLE") {
		re = regexp.MustCompile(`ALTER\s+TABLE\s+([^\s]+)`)
	} else {
		return nil
	}
	matches := re.FindStringSubmatch(statement)
	if len(matches) > 1 {
		return []string{matches[1]}
	}
	return nil
}

var tableContextRegex = regexp.MustCompile(`(?i)\b(FROM|UPDATE|INTO|TABLE)\s+[a-zA-Z0-9_]*$`)

func isTableCompletionContext(text string) bool {
	return tableContextRegex.MatchString(text)
}

func isDataTypeCompletionContext(textBeforeCursor string) bool {
	upperText := strings.ToUpper(textBeforeCursor)

	createTableIndex := strings.LastIndex(upperText, "CREATE TABLE")
	if createTableIndex == -1 {
		return false
	}

	relevantText := textBeforeCursor[createTableIndex:]

	openParens := strings.Count(relevantText, "(")
	closeParens := strings.Count(relevantText, ")")
	if openParens <= closeParens {
		return false
	}

	lastOpenParen := strings.LastIndex(relevantText, "(")
	if lastOpenParen == -1 {
		return false
	}
	contentInParen := relevantText[lastOpenParen+1:]
	parts := strings.Split(contentInParen, ",")
	lastPart := strings.TrimSpace(parts[len(parts)-1])

	if strings.Contains(lastPart, "(") {
		return false
	}

	fields := strings.Fields(lastPart)
	return len(fields) == 2
}

func isConstraintCompletionContext(textBeforeCursor string) bool {
	upperText := strings.ToUpper(textBeforeCursor)

	createTableIndex := strings.LastIndex(upperText, "CREATE TABLE")
	if createTableIndex == -1 {
		return false
	}

	relevantText := textBeforeCursor[createTableIndex:]

	openParens := strings.Count(relevantText, "(")
	closeParens := strings.Count(relevantText, ")")
	if openParens <= closeParens {
		return false
	}

	lastOpenParen := strings.LastIndex(relevantText, "(")
	if lastOpenParen == -1 {
		return false
	}
	contentInParen := relevantText[lastOpenParen+1:]
	parts := strings.Split(contentInParen, ",")
	lastPart := strings.TrimSpace(parts[len(parts)-1])
	fields := strings.Fields(lastPart)

	return len(fields) >= 3
}

func textDocumentCompletion(context *glsp.Context, params *protocol.CompletionParams) (any, error) {
	mutex.RLock()
	content, ok := documentStore[params.TextDocument.URI]
	mutex.RUnlock()
	if !ok {
		return nil, nil
	}

	offset := toOffset(content, params.Position)
	contentBeforeCursor := content[:offset]

	if isDataTypeCompletionContext(contentBeforeCursor) {
		lspLog.Infof("In CREATE TABLE data type completion context for db: %s", lspActiveDbType)
		if dbDataTypes, ok := dataTypeCmp[lspActiveDbType]; ok {
			return createCompletions(dbDataTypes...), nil
		}
	}

	if isConstraintCompletionContext(contentBeforeCursor) {
		lspLog.Info("In CREATE TABLE constraint completion context")
		completions := createCompletions("primary_key", "not_null", "unique", "default", "check", "on_delete")
		if dbConstraints, ok := constraintCmp[lspActiveDbType]; ok {
			completions = append(completions, createCompletions(dbConstraints...)...)
		}
		return completions, nil
	}

	if isTableCompletionContext(contentBeforeCursor) {
		lspLog.Info("In table completion context, suggesting tables.")
		var items []protocol.CompletionItem
		for table := range dbSchemaCache {
			items = append(items, protocol.CompletionItem{
				Label: table,
				Kind:  ptr(protocol.CompletionItemKindClass),
			})
		}
		return items, nil
	}

	currentStatement := isolateCurrentStatement(content, offset)
	tables := extractTableName(currentStatement)

	if len(tables) > 0 {
		lspLog.Infof("Found table(s) in query: %v. Suggesting their columns.", tables)
		var items []protocol.CompletionItem
		for _, table := range tables {
			tableName := strings.Fields(strings.TrimSpace(table))[0]
			if columns, ok := dbSchemaCache[strings.ToLower(tableName)]; ok {
				for _, col := range columns {
					items = append(items, protocol.CompletionItem{
						Label:      col,
						Kind:       ptr(protocol.CompletionItemKindField),
						Detail:     ptr("column in " + tableName),
						InsertText: ptr(col),
					})
				}
			}
		}
		items = append(items, createCompletions(
			"order_by", "add_column", "drop_column", "create_fk", "on_delete",
			"join", "left_join", "right_join", "inner_join", "full_outer_join",
		)...)
		items = append(items, createCompletions(
			"where", "group_by", "limit", "on", "and", "or", "not")...)
		if len(items) > 0 {
			return items, nil
		}
	}

	lspLog.Info("No specific context matched. Returning top-level completions.")
	completions := createCompletions(
		"select_from", "insert_into", "update_set", "delete_from",
		"create_table", "alter_table", "drop_table", "create_table_without_pk",
		"create_index", "create_unique_index", "create_fk", "on_delete",
		"join", "left_join", "right_join", "inner_join", "full_outer_join",
	)
	completions = append(completions, createCompletions(
		"where", "group_by", "limit", "on", "and", "or", "not")...)
	return completions, nil
}

func toOffset(content string, position protocol.Position) int {
	offset := 0
	lines := strings.Split(content, "\n")
	for i := 0; i < int(position.Line); i++ {
		if i < len(lines) {
			offset += len(lines[i]) + 1
		}
	}
	offset += int(position.Character)
	if offset > len(content) {
		return len(content)
	}
	return offset
}

func ptr[T any](v T) *T {
	return &v
}

func createCompletions(keys ...string) []protocol.CompletionItem {
	var items []protocol.CompletionItem
	for _, key := range keys {
		if snippet, ok := CompletionItems[key]; ok {
			items = append(items, protocol.CompletionItem{
				Label:            snippet.Label,
				Kind:             ptr(snippet.Kind),
				InsertTextFormat: ptr(snippet.InsertTextFormat),
				InsertText:       ptr(snippet.InsertText),
			})
		}
	}
	return items
}

type CompletionDetail struct {
	Label            string
	Kind             protocol.CompletionItemKind
	InsertTextFormat protocol.InsertTextFormat
	InsertText       string
}

var constraintCmp = map[string][]string{
	"sqlite":  {"autoincrement"},
	"libsql":  {"autoincrement"},
	"mysql":   {"auto_increment", "unsigned"},
	"mariadb": {"auto_increment", "unsigned"},
}
var dataTypeCmp = map[string][]string{
	"sqlite": {
		"dt_integer", "dt_text", "dt_blob", "dt_real", "dt_numeric",
	},
	"libsql": {
		"dt_integer", "dt_text", "dt_blob", "dt_real", "dt_numeric",
	},
	"postgres": {
		"dt_char", "dt_varchar", "dt_bytea", "dt_text", "dt_json", "dt_jsonb",
		"dt_smallserial", "dt_serial", "dt_bigserial", "dt_bit", "dt_varbit", "dt_smallint", "dt_integer", "dt_bigint", "dt_boolean", "dt_real", "dt_double_precision", "dt_decimal", "dt_money",
		"dt_date", "dt_timestamp", "dt_timestamptz", "dt_time", "dt_interval",
		"dt_point", "dt_line", "dt_lseg", "dt_box", "path", "dt_polygon", "dt_circle",
		"dt_cidr", "dt_inet", "dt_macaddr", "dt_macaddr8",
	},
	"mysql": {
		"dt_char", "dt_varchar", "dt_binary", "dt_varbinary", "dt_tinytext", "dt_text", "dt_mediumtext", "dt_longtext", "dt_tinyblob", "dt_blob", "dt_mediumblob", "dt_longblob",
		"dt_bit", "dt_tinyint", "dt_smallint", "dt_mediumint", "dt_integer", "dt_bigint", "dt_boolean", "dt_float1", "dt_float2", "dt_double", "dt_double_precision", "dt_decimal",
		"dt_date", "dt_datetime", "dt_timestamp", "dt_time", "dt_year",
	},
	"mariadb": {
		"dt_char", "dt_varchar", "dt_binary", "dt_varbinary", "dt_tinytext", "dt_text", "dt_mediumtext", "dt_longtext", "dt_tinyblob", "dt_blob", "dt_mediumblob", "dt_longblob",
		"dt_bit", "dt_tinyint", "dt_smallint", "dt_mediumint", "dt_integer", "dt_bigint", "dt_boolean", "dt_float1", "dt_float2", "dt_double", "dt_double_precision", "dt_decimal",
		"dt_date", "dt_datetime", "dt_timestamp", "dt_time", "dt_year",
	},
}

var CompletionItems = map[string]CompletionDetail{
	// Strings
	"dt_char": {
		Label:            "CHAR",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "CHAR(${1:int})",
	},
	"dt_varchar": {
		Label:            "VARCHAR",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "VARCHAR(${1:int})",
	},
	"dt_binary": {
		Label:            "BINARY",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "BINARY(${1:int})",
	},
	"dt_bytea": {
		Label:            "BYTEA",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "BYTEA",
	},
	"dt_varbinary": {
		Label:            "VARBINARY",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "VARBINARY(${1:int})",
	},
	"dt_tinytext": {
		Label:            "TINYTEXT",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "TINYTEXT",
	},
	"dt_text": {
		Label:            "TEXT",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "TEXT",
	},
	"dt_mediumtext": {
		Label:            "MEDIUMTEXT",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "MEDIUMTEXT",
	},
	"dt_longtext": {
		Label:            "LONGTEXT",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "LONGTEXT",
	},
	"dt_tinyblob": {
		Label:            "TINYBLOB",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "TINYBLOB",
	},
	"dt_blob": {
		Label:            "BLOB",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "BLOB",
	},
	"dt_mediumblob": {
		Label:            "MEDIUMBLOB",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "MEDIUMBLOB",
	},
	"dt_longblob": {
		Label:            "LONGBLOB",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "LONGBLOB",
	},
	"dt_json": {
		Label:            "JSON",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "JSON",
	},
	"dt_jsonb": {
		Label:            "JSONB",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "JSONB",
	},
	// Numeric
	"dt_bit": {
		Label:            "BIT(size)",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "BIT(${1:int})",
	},
	"dt_varbit": {
		Label:            "VARBIT(size)",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "VARBIT(${1:int})",
	},
	"dt_tinyint": {
		Label:            "TINYINT(size)",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "TINYINT(${1:int})",
	},
	"dt_smallint": {
		Label:            "SMALLINT",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "SMALLINT",
	},
	"dt_mediumint": {
		Label:            "MEDIUMINT",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "MEDIUMINT",
	},
	"dt_integer": {
		Label:            "INTEGER",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "INTEGER",
	},
	"dt_bigint": {
		Label:            "BIGINT",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "BIGINT",
	},
	"dt_boolean": {
		Label:            "BOOLEAN",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "BOOLEAN",
	},
	"dt_float1": {
		Label:            "FLOAT",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "FLOAT",
	},
	"dt_float2": {
		Label:            "FLOAT(size,d)",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "FLOAT(${1:int},${2:int})",
	},
	"dt_double": {
		Label:            "DOUBLE(size,d)",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "DOUBLE(${1:int},${2:int})",
	},
	"dt_double_precision": {
		Label:            "DOUBLE PRECISION(size,d)",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "DOUBLE PRECISION(${1:int},${2:int})",
	},
	"dt_decimal": {
		Label:            "DECIMAL",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "DECIMAL(${1:int},${2:int})",
	},
	"dt_real": {
		Label:            "REAL",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "REAL",
	},
	"dt_numeric": {
		Label:            "NUMERIC",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "NUMERIC",
	},
	"dt_smallserial": {
		Label:            "SMALLSERIAL",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "SMALLSERIAL",
	},
	"dt_serial": {
		Label:            "SERIAL",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "SERIAL",
	},
	"dt_bigserial": {
		Label:            "BIGSERIAL",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "BIGSERIAL",
	},
	"dt_money": {
		Label:            "MONEY",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "MONEY",
	},
	// Date/Time
	"dt_date": {
		Label:            "DATE",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "DATE",
	},
	"dt_datetime": {
		Label:            "DATETIME",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "DATETIME",
	},
	"dt_timestamp": {
		Label:            "TIMESTAMP",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "TIMESTAMP",
	},
	"dt_timestamptz": {
		Label:            "TIMESTAMPTZ",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "TIMESTAMPTZ",
	},
	"dt_time": {
		Label:            "TIME",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "TIME",
	},
	"dt_year": {
		Label:            "YEAR",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "YEAR",
	},
	"dt_interval": {
		Label:            "INTERVAL",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "INTERVAL",
	},
	// Geometric
	"dt_point": {
		Label:            "POINT",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "POINT",
	},
	"dt_line": {
		Label:            "LINE",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "LINE",
	},
	"dt_lseg": {
		Label:            "LSEG",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "LSEG",
	},
	"dt_box": {
		Label:            "BOX",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "BOX",
	},
	"dt_path": {
		Label:            "PATH",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "PATH",
	},
	"dt_polygon": {
		Label:            "POLYGON",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "POLYGON",
	},
	"dt_circle": {
		Label:            "CIRCLE",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "CIRCLE",
	},
	// Network
	"dt_cidr": {
		Label:            "CIDR",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "CIDR",
	},
	"dt_inet": {
		Label:            "INET",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "INET",
	},
	"dt_macaddr": {
		Label:            "MACADDR",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "MACADDR",
	},
	"dt_macaddr8": {
		Label:            "MACADDR8",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "MACADDR8",
	},
	// Other
	"dt_uuid": {
		Label:            "UUID",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "UUID",
	},
	"dt_xml": {
		Label:            "XML",
		Kind:             protocol.CompletionItemKindTypeParameter,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "XML",
	},
	// Constraints
	"autoincrement": {
		Label:            "AUTOINCREMENT",
		Kind:             protocol.CompletionItemKindKeyword,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "AUTOINCREMENT",
	},
	"auto_increment": {
		Label:            "AUTO_INCREMENT",
		Kind:             protocol.CompletionItemKindKeyword,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "AUTO_INCREMENT",
	},
	"unsigned": {
		Label:            "UNSIGNED",
		Kind:             protocol.CompletionItemKindKeyword,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "UNSIGNED",
	},
	// Queries
	"select_from": {
		Label:            "SELECT FROM",
		Kind:             protocol.CompletionItemKindSnippet,
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "SELECT ${2:columns} \nFROM ${1:table}",
	},
	"insert_into": {
		Label:            "INSERT INTO",
		Kind:             protocol.CompletionItemKindSnippet,
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "INSERT INTO ${1:table_name} (${2:column1})\nVALUES (${3:value1})",
	},
	"update_set": {
		Label:            "UPDATE SET",
		Kind:             protocol.CompletionItemKindSnippet,
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "UPDATE ${1:table_name}\nSET ${2:column1} = ${3:value1}\nWHERE ${4:condition}",
	},
	"delete_from": {
		Label:            "DELETE FROM",
		Kind:             protocol.CompletionItemKindSnippet,
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "DELETE FROM ${1:table_name} WHERE ${2:condition}",
	},
	"order_by": {
		Label:            "ORDER BY",
		Kind:             protocol.CompletionItemKindSnippet,
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "ORDER BY ${1:column} ${2:ASC|DESC}",
	},
	"create_table": {
		Label:            "CREATE TABLE",
		Kind:             protocol.CompletionItemKindSnippet,
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "CREATE TABLE IF NOT EXISTS ${1:table_name} (\n\t${2:id} ${3:data_type} PRIMARY KEY,\n\t${4:column_name} ${5:data_type}\n);",
	},
	"create_table_without_pk": {
		Label:            "CREATE TABLE without PK",
		Kind:             protocol.CompletionItemKindSnippet,
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "CREATE TABLE IF NOT EXISTS ${1:table_name} (\n\t${2:column_name} ${3:data_type}\n);",
	},
	"alter_table": {
		Label:            "ALTER TABLE",
		Kind:             protocol.CompletionItemKindSnippet,
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "ALTER TABLE ${1:table_name} ",
	},
	"add_column": {
		Label:            "ADD COLUMN",
		Kind:             protocol.CompletionItemKindSnippet,
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "ADD COLUMN ${1:column_name} ${2:data_type}",
	},
	"drop_column": {
		Label:            "DROP COLUMN",
		Kind:             protocol.CompletionItemKindSnippet,
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "DROP COLUMN ${1:column_name}",
	},
	"drop_table": {
		Label:            "DROP TABLE",
		Kind:             protocol.CompletionItemKindSnippet,
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "DROP TABLE ${1:table_name}",
	},
	"create_index": {
		Label:            "CREATE INDEX",
		Kind:             protocol.CompletionItemKindSnippet,
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "CREATE INDEX ${1:index_name} ON ${2:table_name} (${3:column_name})",
	},
	"create_fk": {
		Label:            "FOREIGN KEY",
		Kind:             protocol.CompletionItemKindSnippet,
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "FOREIGN KEY (${1:column}) REFERENCES ${2:table}(${3:column})",
	},
	"create_unique_index": {
		Label:            "CREATE UNIQUE INDEX",
		Kind:             protocol.CompletionItemKindSnippet,
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "CREATE UNIQUE INDEX ${1:index_name} ON ${2:table_name} (${3:column_name})",
	},
	"join": {
		Label:            "JOIN",
		Kind:             protocol.CompletionItemKindSnippet,
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "JOIN ${1:table_name} ON ${2:table_name}.${3:column_name} = ${4:table_name}.${5:column_name}",
	},
	"left_join": {
		Label:            "LEFT JOIN",
		Kind:             protocol.CompletionItemKindSnippet,
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "LEFT JOIN ${1:table_name} ON ${2:table_name}.${3:column_name} = ${4:table_name}.${5:column_name}",
	},
	"right_join": {
		Label:            "RIGHT JOIN",
		Kind:             protocol.CompletionItemKindSnippet,
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "RIGHT JOIN ${1:table_name} ON ${2:table_name}.${3:column_name} = ${4:table_name}.${5:column_name}",
	},
	"inner_join": {
		Label:            "INNER JOIN",
		Kind:             protocol.CompletionItemKindSnippet,
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "INNER JOIN ${1:table_name} ON ${2:table_name}.${3:column_name} = ${4:table_name}.${5:column_name}",
	},
	"full_outer_join": {
		Label:            "FULL OUTER JOIN",
		Kind:             protocol.CompletionItemKindSnippet,
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "FULL OUTER JOIN ${1:table_name} ON ${2:table_name}.${3:column_name} = ${4:table_name}.${5:column_name}",
	},
	"primary_key": {
		Label:            "PRIMARY KEY",
		Kind:             protocol.CompletionItemKindKeyword,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "PRIMARY KEY",
	},
	"not_null": {
		Label:            "NOT NULL",
		Kind:             protocol.CompletionItemKindKeyword,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "NOT NULL",
	},
	"unique": {
		Label:            "UNIQUE",
		Kind:             protocol.CompletionItemKindKeyword,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "UNIQUE",
	},
	"default": {
		Label:            "DEFAULT",
		Kind:             protocol.CompletionItemKindSnippet,
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "DEFAULT ${1:value}",
	},
	"check": {
		Label:            "CHECK",
		Kind:             protocol.CompletionItemKindSnippet,
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "CHECK (${1:value})",
	},
	"on_delete": {
		Label:            "ON DELETE CASCADE",
		Kind:             protocol.CompletionItemKindSnippet,
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "ON DELETE CASCADE",
	},
	"where": {
		Label:            "WHERE",
		Kind:             protocol.CompletionItemKindKeyword,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "WHERE",
	},
	"group_by": {
		Label:            "GROUP BY",
		Kind:             protocol.CompletionItemKindKeyword,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "GROUP BY",
	},
	"limit": {
		Label:            "LIMIT",
		Kind:             protocol.CompletionItemKindKeyword,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "LIMIT",
	},
	"on": {
		Label:            "ON",
		Kind:             protocol.CompletionItemKindKeyword,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "ON",
	},
	"and": {
		Label:            "AND",
		Kind:             protocol.CompletionItemKindKeyword,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "AND",
	},
	"or": {
		Label:            "OR",
		Kind:             protocol.CompletionItemKindKeyword,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "OR",
	},
	"not": {
		Label:            "NOT",
		Kind:             protocol.CompletionItemKindKeyword,
		InsertTextFormat: protocol.InsertTextFormatPlainText,
		InsertText:       "NOT",
	},
}
