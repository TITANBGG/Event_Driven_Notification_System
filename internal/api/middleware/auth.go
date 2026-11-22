package middleware

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"log/slog"
	"net/http"
	"strings"

	"github.com/TITANBGG/notification-system/internal/config"
	"github.com/TITANBGG/notification-system/internal/observability"
)

// Auth failure reasons, used as the only label of http_auth_failures_total (never the key name).
const (
	AuthFailureMissing   = "missing"
	AuthFailureInvalid   = "invalid"
	AuthFailureForbidden = "forbidden"
)

// WSAuthSubprotocol is the WebSocket subprotocol that carries a bearer token from browsers, which
// cannot set an Authorization header on `new WebSocket(...)`: the client offers ["bearer", <token>]
// and the server selects only "bearer", so the token is never echoed back.
const WSAuthSubprotocol = "bearer"

// AuthFailureObserver counts rejected requests by reason.
type AuthFailureObserver interface {
	IncAuthFailure(reason string)
}

// Principal is the authenticated caller of a request.
type Principal struct {
	Name   string
	scopes map[string]bool
}

// HasScope reports whether the principal may use scope; admin implies every scope.
func (p Principal) HasScope(scope string) bool {
	return p.scopes[config.ScopeAdmin] || p.scopes[scope]
}

type principalCtxKey struct{}

// PrincipalFrom returns the authenticated caller, if authentication is enabled and succeeded.
func PrincipalFrom(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalCtxKey{}).(Principal)
	return p, ok
}

type authKey struct {
	hash      [sha256.Size]byte
	principal Principal
}

// Authenticator resolves bearer tokens to principals. Only SHA-256 digests of the secrets are
// kept, and every lookup compares against all keys in constant time so response timing reveals
// neither which key matched nor how much of a guess was right.
type Authenticator struct {
	keys     []authKey
	observer AuthFailureObserver
}

// NewAuthenticator builds an Authenticator from validated config keys. With no keys,
// authentication is disabled and both middlewares become no-ops.
func NewAuthenticator(keys []config.APIKey, observer AuthFailureObserver) *Authenticator {
	a := &Authenticator{observer: observer, keys: make([]authKey, 0, len(keys))}
	for _, k := range keys {
		scopes := make(map[string]bool, len(k.Scopes))
		for _, s := range k.Scopes {
			scopes[s] = true
		}
		a.keys = append(a.keys, authKey{
			hash:      sha256.Sum256([]byte(k.Secret)),
			principal: Principal{Name: k.Name, scopes: scopes},
		})
	}
	return a
}

// Enabled reports whether any API key is configured.
func (a *Authenticator) Enabled() bool {
	return a != nil && len(a.keys) > 0
}

func (a *Authenticator) lookup(token string) (Principal, bool) {
	sum := sha256.Sum256([]byte(token))
	var (
		found Principal
		ok    bool
	)
	// No early return: the loop cost is the same whether the token matches the first key,
	// the last key or none of them.
	for i := range a.keys {
		if subtle.ConstantTimeCompare(sum[:], a.keys[i].hash[:]) == 1 {
			found, ok = a.keys[i].principal, true
		}
	}
	return found, ok
}

func (a *Authenticator) fail(reason string) {
	if a.observer != nil {
		a.observer.IncAuthFailure(reason)
	}
}

// Authenticate requires a valid bearer token and binds the principal to the request context
// and logger. It must run after CorrelationID (errors carry the correlation id) and after CORS
// (preflight requests carry no credentials and are answered before reaching it).
func Authenticate(a *Authenticator) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if !a.Enabled() {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token, present := bearerToken(r)
			if !present {
				a.fail(AuthFailureMissing)
				w.Header().Set("WWW-Authenticate", `Bearer realm="notification-api"`)
				writeMiddlewareError(r.Context(), w, http.StatusUnauthorized, "unauthorized",
					"missing API key: send an Authorization: Bearer header")
				return
			}
			p, ok := a.lookup(token)
			if !ok {
				a.fail(AuthFailureInvalid)
				w.Header().Set("WWW-Authenticate", `Bearer realm="notification-api", error="invalid_token"`)
				writeMiddlewareError(r.Context(), w, http.StatusUnauthorized, "unauthorized", "invalid API key")
				return
			}
			ctx := context.WithValue(r.Context(), principalCtxKey{}, p)
			// The key name (never the secret) is attached to every downstream log line of the request.
			ctx = observability.WithFields(ctx, slog.String("api_key_name", p.Name))
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RequireScope rejects authenticated callers that lack scope with 403. It is a no-op when
// authentication is disabled.
func RequireScope(a *Authenticator, scope string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if !a.Enabled() {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			p, ok := PrincipalFrom(r.Context())
			if !ok {
				// Defensive: RequireScope mounted without Authenticate must fail closed.
				a.fail(AuthFailureMissing)
				w.Header().Set("WWW-Authenticate", `Bearer realm="notification-api"`)
				writeMiddlewareError(r.Context(), w, http.StatusUnauthorized, "unauthorized",
					"missing API key: send an Authorization: Bearer header")
				return
			}
			if !p.HasScope(scope) {
				a.fail(AuthFailureForbidden)
				w.Header().Set("WWW-Authenticate", `Bearer realm="notification-api", error="insufficient_scope", scope="`+scope+`"`)
				writeMiddlewareError(r.Context(), w, http.StatusForbidden, "forbidden",
					"API key lacks the required scope: "+scope)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// bearerToken extracts the credential from "Authorization: Bearer <token>" or, for WebSocket
// upgrades only, from the subprotocol pair "bearer, <token>". present is false when the request
// carries no credential at all (distinguishing "missing" from "invalid" in metrics).
func bearerToken(r *http.Request) (token string, present bool) {
	if h := strings.TrimSpace(r.Header.Get("Authorization")); h != "" {
		scheme, cred, found := strings.Cut(h, " ")
		if !found || !strings.EqualFold(scheme, "Bearer") {
			return "", true
		}
		return strings.TrimSpace(cred), true
	}
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		return "", false
	}
	var protocols []string
	for _, v := range r.Header.Values("Sec-WebSocket-Protocol") {
		for _, p := range strings.Split(v, ",") {
			if p = strings.TrimSpace(p); p != "" {
				protocols = append(protocols, p)
			}
		}
	}
	for i := 0; i+1 < len(protocols); i++ {
		if protocols[i] == WSAuthSubprotocol {
			return protocols[i+1], true
		}
	}
	return "", false
}
