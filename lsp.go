package main

import (
	_ "github.com/tliron/commonlog/simple"
	"github.com/tliron/glsp"
	protocol "github.com/tliron/glsp/protocol_3_16"
)

const lspName = "Schema"

var (
	handler protocol.Handler
)

func initialize(context *glsp.Context, params *protocol.InitializeParams) (any, error) {
	capabilities := handler.CreateServerCapabilities()
	capabilities.CompletionProvider = &protocol.CompletionOptions{}
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

func textDocumentCompletion(context *glsp.Context, params *protocol.CompletionParams) (any, error) {
	var completionItems []protocol.CompletionItem
	for _, snippet := range SnippetMapper {
		completionItems = append(completionItems, protocol.CompletionItem{
			Label:            snippet.Label,
			Kind:             ptr(snippet.Kind),
			Detail:           ptr(snippet.Detail),
			InsertTextFormat: ptr(snippet.InsertTextFormat),
			InsertText:       ptr(snippet.InsertText),
		})
	}
	return completionItems, nil
}

func ptr[T any](v T) *T {
	return &v
}

type CompletionSnippet struct {
	Label            string
	Kind             protocol.CompletionItemKind
	Detail           string
	InsertTextFormat protocol.InsertTextFormat
	InsertText       string
}

var SnippetMapper = map[string]CompletionSnippet{
	"select_from": {
		Label:            "SELECT FROM",
		Kind:             protocol.CompletionItemKindSnippet,
		Detail:           "SELECT FROM statement.",
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "SELECT ${1:columns} \nFROM ${2:table};",
	},
	"insert_into": {
		Label:            "INSERT INTO",
		Kind:             protocol.CompletionItemKindSnippet,
		Detail:           "INSERT INTO statement.",
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "INSERT INTO ${1:table_name} (${2:column1})\nVALUES (${3:value1});",
	},
	"update_set": {
		Label:            "UPDATE SET",
		Kind:             protocol.CompletionItemKindSnippet,
		Detail:           "UPDATE statement.",
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "UPDATE ${1:table_name}\nSET ${2:column1} = ${3:value1}\nWHERE ${4:condition};",
	},
	"delete_from": {
		Label:            "DELETE FROM",
		Kind:             protocol.CompletionItemKindSnippet,
		Detail:           "DELETE FROM statement.",
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "DELETE FROM ${1:table_name} WHERE ${2:condition};",
	},
	"order_by": {
		Label:            "ORDER BY",
		Kind:             protocol.CompletionItemKindSnippet,
		Detail:           "ORDER BY clause.",
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "ORDER BY ${1:column} ${2:ASC|DESC};",
	},
	"create_table": {
		Label:            "CREATE TABLE",
		Kind:             protocol.CompletionItemKindSnippet,
		Detail:           "CREATE TABLE statement.",
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "CREATE TABLE ${1:table_name} (\n\t${2:column_name} ${3:data_type}\n);",
	},
	"alter_table": {
		Label:            "ALTER TABLE",
		Kind:             protocol.CompletionItemKindSnippet,
		Detail:           "ALTER TABLE statement.",
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "ALTER TABLE ${1:table_name} ADD ${2:column_name} ${3:datatype};",
	},
	"drop_table": {
		Label:            "DROP TABLE",
		Kind:             protocol.CompletionItemKindSnippet,
		Detail:           "DROP TABLE statement.",
		InsertTextFormat: protocol.InsertTextFormatSnippet,
		InsertText:       "DROP TABLE ${1:table_name};",
	},
}
