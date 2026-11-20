// Package template implements the safe {{variable}} template renderer and placeholder validator.
//
// WHY: Replacing Go's text/template with a bounded regex/strings.Builder parser eliminates template
// injection vectors (method/pipeline execution) while rendering notifications immutably at create time.
//
// Expected output example (Phase 9):
//
//	Render("Merhaba {{name}}, kod: {{code}}", {"name":"Ayşe","code":"4821"}) -> "Merhaba Ayşe, kod: 4821"
package template
