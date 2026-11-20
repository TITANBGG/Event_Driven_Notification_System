package template_test

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TITANBGG/notification-system/internal/domain"
	"github.com/TITANBGG/notification-system/internal/template"
)

func strPtr(s string) *string { return &s }

func TestParsePlaceholdersAndRenderSubstitution(t *testing.T) {
	t.Parallel()

	t.Run("spaces in braces, repeated variable, adjacent placeholders, primitive formatting", func(t *testing.T) {
		t.Parallel()
		tmpl := domain.Template{
			ID:        uuid.Must(uuid.NewV7()),
			Name:      "otp_sms",
			Channel:   domain.ChannelSMS,
			Body:      "Merhaba {{ name }}, kodunuz: {{code}}{{code}} ({{active}}, {{attempts}})",
			Variables: []string{"name", "code", "active", "attempts"},
		}
		out, ve := template.Render("", tmpl, map[string]any{
			"name":     "Ayşe",
			"code":     4821,
			"active":   true,
			"attempts": 2.5,
			"extraVar": "ignored",
		})
		require.Nil(t, ve)
		assert.Equal(t, "Merhaba Ayşe, kodunuz: 48214821 (true, 2.5)", out.Content)
		assert.Equal(t, []string{"extraVar"}, out.UnusedVariables)

		// Turkish 'ş' in "Ayşe" switches rendered SMS encoding to UCS-2.
		smsInfo := domain.SMSInfo(out.Content)
		assert.Equal(t, domain.SMSEncodingUCS2, smsInfo.Encoding)
		assert.Equal(t, 1, smsInfo.Segments)
	})

	t.Run("missing required variables lists all missing names", func(t *testing.T) {
		t.Parallel()
		tmpl := domain.Template{
			ID:        uuid.Must(uuid.NewV7()),
			Name:      "multi_var",
			Channel:   domain.ChannelSMS,
			Body:      "Hello {{first}} {{last}}, code {{code}}",
			Variables: []string{"first", "last", "code"},
		}
		_, ve := template.Render("", tmpl, map[string]any{
			"first": "Ali",
		})
		require.NotNil(t, ve)
		require.Len(t, ve.Fields, 2)
		assert.Equal(t, "templateVariables.code", ve.Fields[0].Field)
		assert.Equal(t, "missing_variable", ve.Fields[0].Code)
		assert.Contains(t, ve.Fields[0].Message, "code, last")
		assert.Equal(t, "templateVariables.last", ve.Fields[1].Field)
		assert.Equal(t, "missing_variable", ve.Fields[1].Code)
	})

	t.Run("object or array variable values return 422 invalid_variable_type", func(t *testing.T) {
		t.Parallel()
		tmpl := domain.Template{
			ID:        uuid.Must(uuid.NewV7()),
			Name:      "obj_test",
			Channel:   domain.ChannelSMS,
			Body:      "Hi {{user}} {{items}}",
			Variables: []string{"user", "items"},
		}
		_, ve := template.Render("", tmpl, map[string]any{
			"user":  map[string]any{"nested": "bad"},
			"items": []string{"a", "b"},
		})
		require.NotNil(t, ve)
		require.Len(t, ve.Fields, 2)
		assert.Equal(t, "invalid_variable_type", ve.Fields[0].Code)
		assert.Equal(t, "invalid_variable_type", ve.Fields[1].Code)
	})
}

func TestValidateDefinitionSyntaxMismatchAndStaticChannelLimits(t *testing.T) {
	t.Parallel()

	t.Run("valid definition passes", func(t *testing.T) {
		t.Parallel()
		ve := template.ValidateDefinition(template.DefinitionInput{
			Name:      "order_shipped",
			Channel:   "email",
			Subject:   strPtr("Order #{{order_id}} Shipped"),
			Body:      "Dear {{customer_name}}, your order #{{order_id}} is on the way.",
			Variables: []string{"customer_name", "order_id"},
		})
		assert.Nil(t, ve)
	})

	t.Run("malformed braces and invalid variable syntax fail", func(t *testing.T) {
		t.Parallel()
		for _, badBody := range []string{
			"Hello {{123bad}}",
			"Hello {{unclosed",
			"Hello {{}}",
			"Hello {{a.b}}",
			"Hello {{ printf \"%s\" .Name }}",
			"Hello stray }} brace",
		} {
			ve := template.ValidateDefinition(template.DefinitionInput{
				Name:      "bad_syntax",
				Channel:   "sms",
				Body:      badBody,
				Variables: []string{},
			})
			require.NotNil(t, ve, "expected validation failure for %q", badBody)
			assert.Equal(t, "invalid_template_syntax", ve.Fields[0].Code)
		}
	})

	t.Run("placeholder and declared variables mismatch in either direction fails", func(t *testing.T) {
		t.Parallel()
		// Undeclared placeholder in body.
		veUndeclared := template.ValidateDefinition(template.DefinitionInput{
			Name:      "mismatch_1",
			Channel:   "sms",
			Body:      "Hi {{name}} {{code}}",
			Variables: []string{"name"},
		})
		require.NotNil(t, veUndeclared)
		assert.Equal(t, "variables_mismatch", veUndeclared.Fields[0].Code)

		// Declared variable not used in template.
		veUnused := template.ValidateDefinition(template.DefinitionInput{
			Name:      "mismatch_2",
			Channel:   "sms",
			Body:      "Hi {{name}}",
			Variables: []string{"name", "extra"},
		})
		require.NotNil(t, veUnused)
		assert.Equal(t, "variables_mismatch", veUnused.Fields[0].Code)
	})

	t.Run("static parts violating channel limits fail on template create", func(t *testing.T) {
		t.Parallel()
		// SMS static text alone > 10 segments (1531 ASCII chars).
		veSMS := template.ValidateDefinition(template.DefinitionInput{
			Name:      "huge_sms",
			Channel:   "sms",
			Body:      strings.Repeat("a", 1531) + "{{name}}",
			Variables: []string{"name"},
		})
		require.NotNil(t, veSMS)
		assert.Equal(t, "too_many_segments", veSMS.Fields[0].Code)

		// Push static title > 100 chars.
		vePush := template.ValidateDefinition(template.DefinitionInput{
			Name:      "long_push_title",
			Channel:   "push",
			Title:     strPtr(strings.Repeat("T", 101) + "{{name}}"),
			Body:      "Hello {{name}}",
			Variables: []string{"name"},
		})
		require.NotNil(t, vePush)
		assert.Equal(t, "title_too_long", vePush.Fields[0].Code)
	})
}
