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
		TriggerCharacters: []string{",", "."},
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

func formatSql(content string) (string, error) {
	statements := strings.Split(content, ";")
	var formattedStatements []string

	for _, stmt := range statements {
		trimmedStmt := strings.TrimSpace(stmt)
		if trimmedStmt == "" {
			continue
		}

		var formatted string
		var err error
		if strings.HasPrefix(strings.ToUpper(trimmedStmt), "CREATE") {
			formatted, err = formatCreateTable(trimmedStmt)
		} else if strings.HasPrefix(strings.ToUpper(trimmedStmt), "WITH") {
			formatted = trimmedStmt
			err = nil
		} else {
			formatted, err = formatQuery(trimmedStmt)
		}
		if err != nil {
			formatted = trimmedStmt
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
		"UPDATE", "SET",
		"INSERT INTO", "VALUES",
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

	columnDefs := splitOnTopLevelCommas(columnsPart)

	for i, colDef := range columnDefs {
		trimmed := strings.TrimSpace(colDef)
		normalized := reWhitespace.ReplaceAllString(trimmed, " ")
		columnDefs[i] = "\t" + normalized
	}
	formattedColumns := strings.Join(columnDefs, ",\n")
	finalStmt := finalTableNamePart + "\n" + formattedColumns + "\n" + restOfStmt
	return finalStmt, nil
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

func textDocumentCompletion(context *glsp.Context, params *protocol.CompletionParams) (any, error) {
	mutex.RLock()
	content, ok := documentStore[params.TextDocument.URI]
	mutex.RUnlock()
	if !ok {
		return nil, nil
	}

	offset := toOffset(content, params.Position)
	contentBeforeCursor := content[:offset]

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
		if len(items) > 0 {
			return items, nil
		}
	}

	lspLog.Info("No specific context matched. Returning top-level completions.")
	return createSnippetCompletions(
		"select_from", "insert_into", "update_set", "delete_from",
		"create_table", "alter_table", "drop_table",
	), nil
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

func createKeywordCompletions(keys ...string) []protocol.CompletionItem {
	var items []protocol.CompletionItem
	for _, key := range keys {
		if keyword, ok := KeywordMapper[key]; ok {
			items = append(items, protocol.CompletionItem{
				Label: keyword.Label,
				Kind:  ptr(keyword.Kind),
			})
		}
	}
	return items
}

func createSnippetCompletions(keys ...string) []protocol.CompletionItem {
	var items []protocol.CompletionItem
	for _, key := range keys {
		if snippet, ok := SnippetMapper[key]; ok {
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

type CompletionSnippet struct {
	Label            string
	Kind             protocol.CompletionItemKind
	InsertTextFormat protocol.InsertTextFormat
	InsertText       string
}
type CompletionKeyWord struct {
	Label string
	Kind  protocol.CompletionItemKind
}

var KeywordMapper = map[string]CompletionKeyWord{
	"where": {
		Label: "WHERE",
		Kind:  protocol.CompletionItemKindKeyword,
	},
}
var SnippetMapper = map[string]CompletionSnippet{
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
	"create_pk": {
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
}
