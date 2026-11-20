package domain_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/TITANBGG/notification-system/internal/domain"
)

func TestStateMachineFullMatrix(t *testing.T) {
	t.Parallel()

	// Exact 7x7 transition matrix per CLAUDE.md Section 3.
	allowed := map[[2]domain.Status]bool{
		{domain.StatusScheduled, domain.StatusProcessing}: true,
		{domain.StatusScheduled, domain.StatusCancelled}:  true,
		{domain.StatusPending, domain.StatusProcessing}:   true,
		{domain.StatusPending, domain.StatusCancelled}:    true,
		{domain.StatusRetrying, domain.StatusProcessing}:  true,
		{domain.StatusRetrying, domain.StatusCancelled}:   true,
		{domain.StatusProcessing, domain.StatusSent}:      true,
		{domain.StatusProcessing, domain.StatusRetrying}:  true,
		{domain.StatusProcessing, domain.StatusFailed}:    true,
		{domain.StatusProcessing, domain.StatusPending}:   true,
		{domain.StatusFailed, domain.StatusPending}:       true,
	}

	statuses := domain.AllStatuses()
	require.Len(t, statuses, 7)

	for _, from := range statuses {
		for _, to := range statuses {
			name := fmt.Sprintf("%s_to_%s", from, to)
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				want := allowed[[2]domain.Status{from, to}]
				assert.Equal(t, want, domain.CanTransition(from, to))
			})
		}
	}
}

func TestAllowedFromAndCancellable(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []domain.Status{domain.StatusProcessing, domain.StatusFailed}, domain.AllowedFrom(domain.StatusPending))
	assert.Equal(t, []domain.Status{domain.StatusScheduled, domain.StatusPending, domain.StatusRetrying}, domain.AllowedFrom(domain.StatusProcessing))
	assert.Equal(t, []domain.Status{domain.StatusScheduled, domain.StatusPending, domain.StatusRetrying}, domain.AllowedFrom(domain.StatusCancelled))
	assert.Equal(t, []domain.Status{domain.StatusProcessing}, domain.AllowedFrom(domain.StatusSent))
	assert.Equal(t, []domain.Status{domain.StatusProcessing}, domain.AllowedFrom(domain.StatusFailed))
	assert.Equal(t, []domain.Status{domain.StatusProcessing}, domain.AllowedFrom(domain.StatusRetrying))
	assert.Empty(t, domain.AllowedFrom(domain.StatusScheduled))

	cancellable := map[domain.Status]bool{
		domain.StatusScheduled:  true,
		domain.StatusPending:    true,
		domain.StatusRetrying:   true,
		domain.StatusProcessing: false,
		domain.StatusSent:       false,
		domain.StatusFailed:     false,
		domain.StatusCancelled:  false,
	}
	for s, want := range cancellable {
		assert.Equal(t, want, domain.IsCancellable(s), "status=%s", s)
	}
}

func TestEnumParsers(t *testing.T) {
	t.Parallel()

	ch, err := domain.ParseChannel(" SMS ")
	require.NoError(t, err)
	assert.Equal(t, domain.ChannelSMS, ch)
	_, err = domain.ParseChannel("whatsapp")
	require.Error(t, err)

	pr, err := domain.ParsePriorityOrDefault("")
	require.NoError(t, err)
	assert.Equal(t, domain.PriorityNormal, pr)
	_, err = domain.ParsePriority("urgent")
	require.Error(t, err)

	st, err := domain.ParseStatus("retrying")
	require.NoError(t, err)
	assert.Equal(t, domain.StatusRetrying, st)
	_, err = domain.ParseStatus("queued")
	require.Error(t, err)

	assert.True(t, domain.OutcomeAbandoned.Valid())
	assert.False(t, domain.AttemptOutcome("unknown").Valid())
}
