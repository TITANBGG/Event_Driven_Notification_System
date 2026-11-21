package template_test

import (
	"testing"

	"github.com/google/uuid"

	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/template"
)

func FuzzTemplateEngine(f *testing.F) {
	f.Add("otp", "sms", "", "", "Your code is {{code}}!", "code", "123456")
	f.Add("welcome", "email", "Hi {{ name }}", "", "Body {{name}} {{extra}}", "name", "Ali")
	f.Add("push_alert", "push", "", "Title {{x}}", "Msg {{y}}", "x", "ok")
	f.Add("broken", "sms", "", "", "Unclosed {{foo and }} bar", "foo", "1")
	f.Add("nested", "email", "{{{{a}}}}", "{{}}", "{{ 1bad }}", "", "")
	f.Add("unicode", "sms", "", "", "Merhaba {{isim}} 🚀", "isim", "Şeyma")

	f.Fuzz(func(
		t *testing.T,
		name, channel, subject, title, body, varKey, varVal string,
	) {
		_, _, _ = template.ParsePlaceholders(body)
		_, _, _ = template.ParsePlaceholders(subject)
		_, _, _ = template.ParsePlaceholders(title)

		var subjPtr, titlePtr *string
		if subject != "" {
			subjPtr = &subject
		}
		if title != "" {
			titlePtr = &title
		}

		varsList := []string{}
		if varKey != "" {
			varsList = append(varsList, varKey)
		}

		def := template.DefinitionInput{
			Name:      name,
			Channel:   channel,
			Subject:   subjPtr,
			Title:     titlePtr,
			Body:      body,
			Variables: varsList,
		}
		ve := template.ValidateDefinition(def)
		if ve != nil && !ve.HasErrors() {
			t.Fatalf("non-nil ValidationError has zero fields")
		}

		ch, chErr := domain.ParseChannel(channel)
		if chErr != nil {
			ch = domain.ChannelSMS
		}

		discovered, _, parseErr := template.ParsePlaceholders(body)
		tmplVars := varsList
		if parseErr == nil && len(discovered) > 0 {
			tmplVars = discovered
		}

		tmpl := domain.Template{
			ID:        uuid.New(),
			Name:      name,
			Channel:   ch,
			Subject:   subjPtr,
			Title:     titlePtr,
			Body:      body,
			Variables: tmplVars,
		}

		varsMap := map[string]any{}
		for _, k := range tmplVars {
			varsMap[k] = varVal
		}
		if varKey != "" {
			varsMap[varKey] = varVal
		}

		out, renderErr := template.Render("", tmpl, varsMap)
		if renderErr != nil && !renderErr.HasErrors() {
			t.Fatalf("non-nil Render ValidationError has zero fields")
		}
		if renderErr == nil {
			_ = template.RenderOutput(out)
		}
	})
}
