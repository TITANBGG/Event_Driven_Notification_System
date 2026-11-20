package template

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/TITANBGG/notification-system/internal/domain"
)

const (
	maxEmailSubjectRunes = 200
	maxEmailContentBytes = 102400

	maxPushTitleRunes   = 100
	maxPushContentRunes = 2000
	maxPushPayloadBytes = 4096
)

var validPlaceholderPattern = regexp.MustCompile(`^\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}$`)

// DefinitionInput carries raw template fields validated on POST/PUT /api/v1/templates.
type DefinitionInput struct {
	Name      string
	Channel   string
	Subject   *string
	Title     *string
	Body      string
	Variables []string
}

// RenderOutput holds the substituted template fields along with any extra unused variable names.
type RenderOutput struct {
	Subject         *string
	Title           *string
	Content         string
	UnusedVariables []string
}

// ParsePlaceholders scans text for {{name}} placeholders using a deterministic scanner (no text/template execution),
// returning sorted unique variable names and the static text with all placeholders stripped.
// Any malformed '{{' token or unclosed}}' returns a descriptive syntax error.
func ParsePlaceholders(text string) (placeholders []string, staticOnly string, err error) {
	seen := make(map[string]struct{})
	var staticBuf strings.Builder
	i := 0
	for i < len(text) {
		openIdx := strings.Index(text[i:], "{{")
		relClose := strings.Index(text[i:], "}}")
		if relClose != -1 && (openIdx == -1 || relClose < openIdx) {
			return nil, "", fmt.Errorf("unmatched closing braces '}}' at byte offset %d", i+relClose)
		}
		if openIdx == -1 {
			staticBuf.WriteString(text[i:])
			break
		}

		absOpen := i + openIdx
		staticBuf.WriteString(text[i:absOpen])

		closeOffset := strings.Index(text[absOpen+2:], "}}")
		if closeOffset == -1 {
			return nil, "", fmt.Errorf("unclosed placeholder '{{' at byte offset %d", absOpen)
		}
		absClose := absOpen + 2 + closeOffset + 2
		token := text[absOpen:absClose]

		m := validPlaceholderPattern.FindStringSubmatch(token)
		if len(m) != 2 {
			return nil, "", fmt.Errorf("invalid placeholder syntax %q: variable name must match [a-zA-Z_][a-zA-Z0-9_]*", token)
		}
		varName := m[1]
		if _, ok := seen[varName]; !ok {
			seen[varName] = struct{}{}
			placeholders = append(placeholders, varName)
		}
		i = absClose
	}
	slices.Sort(placeholders)
	return placeholders, staticBuf.String(), nil
}

// ValidateDefinition validates a template create/update payload:
//   - name and body required; channel valid; subject/title channel appropriateness
//   - valid placeholder syntax in subject, title, and body
//   - exact bidirectional set equality between discovered placeholders and declared variables
//   - static text alone does not already violate channel limits (e.g. >10 SMS segments, >100 push title)
func ValidateDefinition(in DefinitionInput) *domain.ValidationError {
	ve := &domain.ValidationError{}

	if strings.TrimSpace(in.Name) == "" {
		ve.Add("name", "required", "name is required")
	}

	channel, chErr := domain.ParseChannel(in.Channel)
	if strings.TrimSpace(in.Channel) == "" {
		ve.Add("channel", "required", "channel is required")
	} else if chErr != nil {
		ve.Add("channel", "invalid_channel", "must be one of sms, email, push")
	}

	if strings.TrimSpace(in.Body) == "" {
		ve.Add("body", "required", "body is required")
	}

	discovered := make(map[string]struct{})
	var staticSubject, staticTitle, staticBody string

	if in.Subject != nil {
		vars, staticText, err := ParsePlaceholders(*in.Subject)
		if err != nil {
			ve.Add("subject", "invalid_template_syntax", err.Error())
		} else {
			staticSubject = staticText
			for _, v := range vars {
				discovered[v] = struct{}{}
			}
		}
	}

	if in.Title != nil {
		vars, staticText, err := ParsePlaceholders(*in.Title)
		if err != nil {
			ve.Add("title", "invalid_template_syntax", err.Error())
		} else {
			staticTitle = staticText
			for _, v := range vars {
				discovered[v] = struct{}{}
			}
		}
	}

	if strings.TrimSpace(in.Body) != "" {
		vars, staticText, err := ParsePlaceholders(in.Body)
		if err != nil {
			ve.Add("body", "invalid_template_syntax", err.Error())
		} else {
			staticBody = staticText
			for _, v := range vars {
				discovered[v] = struct{}{}
			}
		}
	}

	varNamePattern := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
	declaredSet := make(map[string]struct{}, len(in.Variables))
	for idx, rawVar := range in.Variables {
		v := strings.TrimSpace(rawVar)
		if !varNamePattern.MatchString(v) {
			ve.Add(fmt.Sprintf("variables[%d]", idx), "invalid_variable_name", "variable name must match [a-zA-Z_][a-zA-Z0-9_]*")
			continue
		}
		if _, dup := declaredSet[v]; dup {
			ve.Add("variables", "duplicate_variable", fmt.Sprintf("variable %q is declared more than once", v))
			continue
		}
		declaredSet[v] = struct{}{}
	}

	// Bidirectional check between discovered placeholders and declared variables.
	if !hasSyntaxErrors(ve) {
		var undeclared []string
		for v := range discovered {
			if _, ok := declaredSet[v]; !ok {
				undeclared = append(undeclared, v)
			}
		}
		slices.Sort(undeclared)

		var unusedDeclared []string
		for v := range declaredSet {
			if _, ok := discovered[v]; !ok {
				unusedDeclared = append(unusedDeclared, v)
			}
		}
		slices.Sort(unusedDeclared)

		if len(undeclared) > 0 || len(unusedDeclared) > 0 {
			ve.Add(
				"variables",
				"variables_mismatch",
				fmt.Sprintf("declared variables must match template placeholders (undeclared=%v, unusedDeclared=%v)", undeclared, unusedDeclared),
			)
		}
	}

	if chErr == nil {
		validateStaticChannelLimits(channel, in.Subject, in.Title, staticSubject, staticTitle, staticBody, ve)
	}

	if !ve.HasErrors() {
		return nil
	}
	return ve
}

func hasSyntaxErrors(ve *domain.ValidationError) bool {
	for _, f := range ve.Fields {
		if f.Code == "invalid_template_syntax" || f.Code == "invalid_variable_name" {
			return true
		}
	}
	return false
}

func validateStaticChannelLimits(
	channel domain.Channel,
	rawSubject, rawTitle *string,
	staticSubject, staticTitle, staticBody string,
	ve *domain.ValidationError,
) {
	switch channel {
	case domain.ChannelSMS:
		if rawSubject != nil {
			ve.Add("subject", "unsupported_field", "subject is only supported for email channel")
		}
		if rawTitle != nil {
			ve.Add("title", "unsupported_field", "title is only supported for push channel")
		}
		if staticBody != "" {
			info := domain.SMSInfo(staticBody)
			if info.Segments > domain.MaxSMSSegments {
				ve.Add(
					"body",
					"too_many_segments",
					fmt.Sprintf("static SMS text alone requires %d segments (%s), max %d", info.Segments, info.Encoding, domain.MaxSMSSegments),
				)
			}
		}

	case domain.ChannelEmail:
		if rawTitle != nil {
			ve.Add("title", "unsupported_field", "title is only supported for push channel")
		}
		if rawSubject != nil {
			if strings.ContainsAny(*rawSubject, "\r\n") {
				ve.Add("subject", "invalid_subject", "subject must not contain CR or LF characters")
			}
			if utf8.RuneCountInString(staticSubject) > maxEmailSubjectRunes {
				ve.Add("subject", "subject_too_long", "static email subject exceeds 200 characters")
			}
		}
		if len(staticBody) > maxEmailContentBytes {
			ve.Add("body", "content_too_large", "static email body exceeds 102400 bytes")
		}

	case domain.ChannelPush:
		if rawSubject != nil {
			ve.Add("subject", "unsupported_field", "subject is only supported for email channel")
		}
		if rawTitle != nil && utf8.RuneCountInString(staticTitle) > maxPushTitleRunes {
			ve.Add("title", "title_too_long", "static push title exceeds 100 characters")
		}
		if utf8.RuneCountInString(staticBody) > maxPushContentRunes {
			ve.Add("body", "content_too_long", "static push body exceeds 2000 characters")
		}
		payload := struct {
			To      string  `json:"to"`
			Channel string  `json:"channel"`
			Content string  `json:"content"`
			Title   *string `json:"title,omitempty"`
		}{
			To:      "t",
			Channel: string(domain.ChannelPush),
			Content: staticBody,
		}
		if staticTitle != "" {
			payload.Title = &staticTitle
		}
		if encoded, err := json.Marshal(payload); err == nil && len(encoded) > maxPushPayloadBytes {
			ve.Add("body", "payload_too_large", fmt.Sprintf("static push payload is %d bytes, max %d", len(encoded), maxPushPayloadBytes))
		}
	}
}

// Render substitutes {{name}} placeholders across a template's subject, title, and body.
// It checks that every declared variable is provided, rejects non-primitive values (objects, arrays, nil),
// formats strings/numbers/booleans via strconv, and reports any extra provided keys in UnusedVariables.
func Render(prefix string, tmpl domain.Template, vars map[string]any) (RenderOutput, *domain.ValidationError) {
	ve := &domain.ValidationError{}
	varsField := joinField(prefix, "templateVariables")
	if prefix == "preview." {
		varsField = "variables"
	}

	declaredSet := make(map[string]struct{}, len(tmpl.Variables))
	var missing []string
	for _, name := range tmpl.Variables {
		declaredSet[name] = struct{}{}
		val, exists := vars[name]
		if !exists || val == nil {
			missing = append(missing, name)
		}
	}
	slices.Sort(missing)
	for _, name := range missing {
		ve.Add(
			fmt.Sprintf("%s.%s", varsField, name),
			"missing_variable",
			fmt.Sprintf("missing required template variable %q (all missing: %s)", name, strings.Join(missing, ", ")),
		)
	}

	formatted := make(map[string]string, len(vars))
	var unused []string
	keys := make([]string, 0, len(vars))
	for k := range vars {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	for _, k := range keys {
		val := vars[k]
		_, isDeclared := declaredSet[k]
		if !isDeclared {
			unused = append(unused, k)
		}
		if val == nil {
			if !isDeclared {
				ve.Add(fmt.Sprintf("%s.%s", varsField, k), "invalid_variable_type", "template variable value must be a string, number, or boolean")
			}
			continue
		}
		strVal, ok := formatPrimitive(val)
		if !ok {
			ve.Add(
				fmt.Sprintf("%s.%s", varsField, k),
				"invalid_variable_type",
				fmt.Sprintf("template variable %q must be a string, number, or boolean (objects and arrays are not allowed)", k),
			)
			continue
		}
		formatted[k] = strVal
	}

	if ve.HasErrors() {
		return RenderOutput{}, ve
	}

	var outSubject *string
	if tmpl.Subject != nil {
		s, err := substituteString(*tmpl.Subject, formatted)
		if err != nil {
			ve.Add(joinField(prefix, "subject"), "invalid_template_syntax", err.Error())
		} else {
			outSubject = &s
		}
	}

	var outTitle *string
	if tmpl.Title != nil {
		t, err := substituteString(*tmpl.Title, formatted)
		if err != nil {
			ve.Add(joinField(prefix, "title"), "invalid_template_syntax", err.Error())
		} else {
			outTitle = &t
		}
	}

	outBody, err := substituteString(tmpl.Body, formatted)
	if err != nil {
		ve.Add(joinField(prefix, "body"), "invalid_template_syntax", err.Error())
	}

	if ve.HasErrors() {
		return RenderOutput{}, ve
	}

	if unused == nil {
		unused = []string{}
	}
	return RenderOutput{
		Subject:         outSubject,
		Title:           outTitle,
		Content:         outBody,
		UnusedVariables: unused,
	}, nil
}

func substituteString(text string, values map[string]string) (string, error) {
	var buf strings.Builder
	buf.Grow(len(text))
	i := 0
	for i < len(text) {
		openIdx := strings.Index(text[i:], "{{")
		if openIdx == -1 {
			buf.WriteString(text[i:])
			break
		}
		absOpen := i + openIdx
		buf.WriteString(text[i:absOpen])

		closeOffset := strings.Index(text[absOpen+2:], "}}")
		if closeOffset == -1 {
			return "", fmt.Errorf("unclosed placeholder '{{' at byte offset %d", absOpen)
		}
		absClose := absOpen + 2 + closeOffset + 2
		token := text[absOpen:absClose]

		m := validPlaceholderPattern.FindStringSubmatch(token)
		if len(m) != 2 {
			return "", fmt.Errorf("invalid placeholder syntax %q", token)
		}
		buf.WriteString(values[m[1]])
		i = absClose
	}
	return buf.String(), nil
}

func formatPrimitive(v any) (string, bool) {
	switch typed := v.(type) {
	case string:
		return typed, true
	case bool:
		return strconv.FormatBool(typed), true
	case json.Number:
		return typed.String(), true
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64), true
	case float32:
		return strconv.FormatFloat(float64(typed), 'f', -1, 32), true
	case int:
		return strconv.Itoa(typed), true
	case int64:
		return strconv.FormatInt(typed, 10), true
	case int32:
		return strconv.FormatInt(int64(typed), 10), true
	case uint:
		return strconv.FormatUint(uint64(typed), 10), true
	case uint64:
		return strconv.FormatUint(typed, 10), true
	case uint32:
		return strconv.FormatUint(uint64(typed), 10), true
	default:
		return "", false
	}
}

func joinField(prefix, field string) string {
	if prefix == "" || prefix == "preview." {
		return field
	}
	return prefix + field
}
