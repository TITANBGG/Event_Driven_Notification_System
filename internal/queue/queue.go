package queue

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/TITANBGG/notification-system/internal/domain"
)

//go:embed lua/*.lua
var luaFS embed.FS

func mustLoadScript(name string) *redis.Script {
	src, err := luaFS.ReadFile("lua/" + name)
	if err != nil {
		panic(fmt.Sprintf("embedded lua script %q missing: %v", name, err))
	}
	return redis.NewScript(string(src))
}

var (
	enqueueScript      = mustLoadScript("enqueue.lua")
	forceEnqueueScript = mustLoadScript("force_enqueue.lua")
	scheduleScript     = mustLoadScript("schedule.lua")
	dequeueScript      = mustLoadScript("dequeue.lua")
	ackScript          = mustLoadScript("ack.lua")
	deadletterScript   = mustLoadScript("deadletter.lua")
	requeueScript      = mustLoadScript("requeue.lua")
	requeueIDsScript   = mustLoadScript("requeue_ids.lua")
	moveDueScript      = mustLoadScript("move_due.lua")
	unlockScript       = mustLoadScript("unlock.lua")
	extendLockScript   = mustLoadScript("extend_lock.lua")
)

var weightedSlotSchedule = [10]domain.Priority{
	domain.PriorityHigh,
	domain.PriorityHigh,
	domain.PriorityHigh,
	domain.PriorityHigh,
	domain.PriorityHigh,
	domain.PriorityHigh,
	domain.PriorityNormal,
	domain.PriorityNormal,
	domain.PriorityNormal,
	domain.PriorityLow,
}

// Meta is the compact JSON payload stored in {notif}:msg so queues hold only UUIDs
// while workers and the delayed mover retain channel, priority, and trace metadata.
type Meta struct {
	ID            uuid.UUID       `json:"id"`
	Channel       domain.Channel  `json:"channel"`
	Priority      domain.Priority `json:"priority"`
	CorrelationID string          `json:"correlationId,omitempty"`
	TraceParent   string          `json:"traceparent,omitempty"`
	// TraceLink makes the worker start a new trace linked to TraceParent instead of a child span.
	// Set for batch items: as children, 1,000 items put ~28,000 spans into one trace (~145 MB in
	// Jaeger's memory, unreadable in the UI, beyond most backends' per-trace span limits).
	TraceLink bool `json:"traceLink,omitempty"`
}

// MetricsDepths captures live queue lengths across channels, priorities, delayed ZSET, DLQ, and inflight lists.
type MetricsDepths struct {
	ByChannel map[domain.Channel]map[domain.Priority]int64
	Delayed   int64
	DLQ       int64
	Inflight  int64
}

// PriorityOrderForSlot computes the 3-key dequeue probe order for the 6:3:1 weighted priority
// schedule with deterministic fall-through (high > normal > low for remaining slots).
func PriorityOrderForSlot(slot int) [3]domain.Priority {
	idx := ((slot % len(weightedSlotSchedule)) + len(weightedSlotSchedule)) % len(weightedSlotSchedule)
	preferred := weightedSlotSchedule[idx]
	switch preferred {
	case domain.PriorityHigh:
		return [3]domain.Priority{domain.PriorityHigh, domain.PriorityNormal, domain.PriorityLow}
	case domain.PriorityNormal:
		return [3]domain.Priority{domain.PriorityNormal, domain.PriorityHigh, domain.PriorityLow}
	default:
		return [3]domain.Priority{domain.PriorityLow, domain.PriorityHigh, domain.PriorityNormal}
	}
}

// NewClient constructs a configured *redis.Client matching pool size and timeout bounds.
func NewClient(addr string, poolSize int, opts ...ClientOption) *redis.Client {
	o := &redis.Options{
		Addr:         addr,
		PoolSize:     poolSize,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolTimeout:  4 * time.Second,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(o)
		}
	}
	return redis.NewClient(o)
}

// ClientOption customizes the Redis client built by NewClient.
type ClientOption func(*redis.Options)

// WithPassword authenticates every connection (Redis AUTH / requirepass). Empty means no AUTH.
func WithPassword(password string) ClientOption {
	return func(o *redis.Options) { o.Password = password }
}

// Queue manages Redis priority queues, per-worker inflight lists, delayed scheduling, and DLQ operations.
type Queue struct {
	rdb       redis.UniversalClient
	dlqMaxLen int64

	slotMu   sync.Mutex
	slotByCh map[domain.Channel]*atomic.Uint64
}

// New constructs a Queue backed by a go-redis client.
func New(rdb redis.UniversalClient) *Queue {
	return &Queue{
		rdb:       rdb,
		dlqMaxLen: DefaultDLQMaxLen,
		slotByCh: map[domain.Channel]*atomic.Uint64{
			domain.ChannelSMS:   {},
			domain.ChannelEmail: {},
			domain.ChannelPush:  {},
		},
	}
}

// Ping verifies Redis connectivity within ctx's deadline.
func (q *Queue) Ping(ctx context.Context) error {
	if q.rdb == nil {
		return errors.New("redis client is nil")
	}
	return q.rdb.Ping(ctx).Err()
}

// Enqueue idempotently pushes one or more notification IDs onto their target channel/priority
// queues in a single pipelined round trip, retrying once after SCRIPT LOAD on cold Redis nodes.
func (q *Queue) Enqueue(ctx context.Context, items ...Meta) error {
	return q.enqueueBatch(ctx, enqueueScript, "enqueue", items)
}

// ForceEnqueue unconditionally sets tracking metadata and pushes one or more notification IDs onto
// their target channel/priority queues. Used by reconciler recovery when PostgreSQL shows stale pending.
func (q *Queue) ForceEnqueue(ctx context.Context, items ...Meta) error {
	return q.enqueueBatch(ctx, forceEnqueueScript, "force_enqueue", items)
}

func (q *Queue) enqueueBatch(ctx context.Context, script *redis.Script, opName string, items []Meta) error {
	if len(items) == 0 {
		return nil
	}
	if q.rdb == nil {
		return errors.New("redis client is nil")
	}

	payloads := make([][]byte, len(items))
	qKeys := make([]string, len(items))
	for i, item := range items {
		if !item.Channel.Valid() {
			return fmt.Errorf("%s invalid channel %q for %s", opName, item.Channel, item.ID)
		}
		if !item.Priority.Valid() {
			item.Priority = domain.PriorityNormal
		}
		raw, err := json.Marshal(item)
		if err != nil {
			return fmt.Errorf("marshaling queue meta for %s: %w", item.ID, err)
		}
		payloads[i] = raw
		qKeys[i] = Key(item.Channel, item.Priority)
	}

	runPipe := func() ([]*redis.Cmd, error) {
		pipe := q.rdb.Pipeline()
		cmds := make([]*redis.Cmd, len(items))
		for i, item := range items {
			cmds[i] = script.EvalSha(ctx, pipe, []string{MsgHashKey, qKeys[i]}, item.ID.String(), string(payloads[i]))
		}
		_, err := pipe.Exec(ctx)
		return cmds, err
	}

	cmds, err := runPipe()
	if err != nil && isNoScriptError(err, cmds) {
		if loadErr := script.Load(ctx, q.rdb).Err(); loadErr != nil {
			return fmt.Errorf("loading %s script: %w", opName, loadErr)
		}
		_, err = runPipe()
	}
	if err != nil {
		return fmt.Errorf("executing %s pipeline: %w", opName, err)
	}
	return nil
}

// Dequeue atomically moves one item from ch's weighted priority queues into workerID's inflight list.
// Returns (nil, false, nil) when all three priority lists for ch are empty.
func (q *Queue) Dequeue(ctx context.Context, ch domain.Channel, workerID string) (*Meta, bool, error) {
	if q.rdb == nil {
		return nil, false, errors.New("redis client is nil")
	}
	if !ch.Valid() {
		return nil, false, fmt.Errorf("invalid channel %q", ch)
	}

	slot := q.nextSlot(ch)
	order := PriorityOrderForSlot(slot)
	keys := []string{
		MsgHashKey,
		InflightKey(workerID),
		Key(ch, order[0]),
		Key(ch, order[1]),
		Key(ch, order[2]),
	}

	res, err := dequeueScript.Run(ctx, q.rdb, keys).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("running dequeue script: %w", err)
	}

	pair, ok := res.([]any)
	if !ok || len(pair) != 2 {
		return nil, false, fmt.Errorf("unexpected dequeue script result type %T", res)
	}
	idStr, _ := pair[0].(string)
	metaRaw, _ := pair[1].(string)

	id, parseErr := uuid.Parse(idStr)
	if parseErr != nil {
		return nil, false, fmt.Errorf("parsing dequeued id %q: %w", idStr, parseErr)
	}

	if metaRaw != "" {
		var meta Meta
		if unmarshalErr := json.Unmarshal([]byte(metaRaw), &meta); unmarshalErr == nil {
			return &meta, true, nil
		}
	}

	// Defensive fallback if queue list contained an ID whose msg hash entry was cleared.
	return &Meta{
		ID:       id,
		Channel:  ch,
		Priority: order[0],
	}, true, nil
}

// Ack removes id from workerID's inflight list and deletes its tracking metadata from {notif}:msg.
func (q *Queue) Ack(ctx context.Context, workerID string, id uuid.UUID) error {
	if q.rdb == nil {
		return errors.New("redis client is nil")
	}
	if err := ackScript.Run(ctx, q.rdb, []string{MsgHashKey, InflightKey(workerID)}, id.String()).Err(); err != nil {
		return fmt.Errorf("acknowledging notification %s: %w", id, err)
	}
	return nil
}

// Schedule upserts metadata and places id into {notif}:delayed at dueAt while optionally removing
// it from workerID's inflight list.
func (q *Queue) Schedule(ctx context.Context, meta Meta, dueAt time.Time, workerID string) error {
	if q.rdb == nil {
		return errors.New("redis client is nil")
	}
	if !meta.Channel.Valid() {
		return fmt.Errorf("schedule invalid channel %q for %s", meta.Channel, meta.ID)
	}
	if !meta.Priority.Valid() {
		meta.Priority = domain.PriorityNormal
	}

	raw, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshaling schedule meta: %w", err)
	}
	keys := []string{MsgHashKey, DelayedZSetKey}
	if workerID != "" {
		keys = append(keys, InflightKey(workerID))
	}
	dueMs := strconv.FormatInt(dueAt.UTC().UnixMilli(), 10)
	if err := scheduleScript.Run(ctx, q.rdb, keys, meta.ID.String(), string(raw), dueMs).Err(); err != nil {
		return fmt.Errorf("scheduling notification %s: %w", meta.ID, err)
	}
	return nil
}

// ScheduleBatch upserts tracking metadata and places one or more scheduled notification IDs into
// {notif}:delayed at their target due times in a single pipelined round trip.
func (q *Queue) ScheduleBatch(ctx context.Context, metas []Meta, dueAts []time.Time) error {
	if len(metas) == 0 {
		return nil
	}
	if len(metas) != len(dueAts) {
		return fmt.Errorf("schedule batch length mismatch: %d metas vs %d due times", len(metas), len(dueAts))
	}
	if q.rdb == nil {
		return errors.New("redis client is nil")
	}

	payloads := make([][]byte, len(metas))
	dueMillis := make([]string, len(metas))
	for i := range metas {
		if !metas[i].Channel.Valid() {
			return fmt.Errorf("schedule invalid channel %q for %s", metas[i].Channel, metas[i].ID)
		}
		if !metas[i].Priority.Valid() {
			metas[i].Priority = domain.PriorityNormal
		}
		raw, err := json.Marshal(metas[i])
		if err != nil {
			return fmt.Errorf("marshaling schedule meta for %s: %w", metas[i].ID, err)
		}
		payloads[i] = raw
		dueMillis[i] = strconv.FormatInt(dueAts[i].UTC().UnixMilli(), 10)
	}

	keys := []string{MsgHashKey, DelayedZSetKey}
	runPipe := func() ([]*redis.Cmd, error) {
		pipe := q.rdb.Pipeline()
		cmds := make([]*redis.Cmd, len(metas))
		for i, meta := range metas {
			cmds[i] = scheduleScript.EvalSha(ctx, pipe, keys, meta.ID.String(), string(payloads[i]), dueMillis[i])
		}
		_, err := pipe.Exec(ctx)
		return cmds, err
	}

	cmds, err := runPipe()
	if err != nil && isNoScriptError(err, cmds) {
		if loadErr := scheduleScript.Load(ctx, q.rdb).Err(); loadErr != nil {
			return fmt.Errorf("loading schedule script: %w", loadErr)
		}
		_, err = runPipe()
	}
	if err != nil {
		return fmt.Errorf("executing schedule pipeline: %w", err)
	}
	return nil
}

// CancelDelayed best-effort removes cancelled notification IDs from {notif}:delayed and deletes
// their tracking metadata from {notif}:msg to reclaim Redis memory immediately.
// PostgreSQL status='cancelled' remains the authoritative guard (Claim skips cancelled rows).
func (q *Queue) CancelDelayed(ctx context.Context, ids ...uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	if q.rdb == nil {
		return errors.New("redis client is nil")
	}

	members := make([]any, len(ids))
	fields := make([]string, len(ids))
	for i, id := range ids {
		s := id.String()
		members[i] = s
		fields[i] = s
	}

	pipe := q.rdb.Pipeline()
	pipe.ZRem(ctx, DelayedZSetKey, members...)
	pipe.HDel(ctx, MsgHashKey, fields...)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("cleaning cancelled delayed entries: %w", err)
	}
	return nil
}

// RemoveFromDLQ deletes every occurrence of the given IDs from the {notif}:dlq list in one pipelined
// round trip. Used after a manual retry so the list (and dlq_depth) only holds currently failed IDs.
func (q *Queue) RemoveFromDLQ(ctx context.Context, ids ...uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	if q.rdb == nil {
		return errors.New("redis client is nil")
	}
	pipe := q.rdb.Pipeline()
	for _, id := range ids {
		pipe.LRem(ctx, DLQListKey, 0, id.String())
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("removing notifications from DLQ list: %w", err)
	}
	return nil
}

// DeadLetter acknowledges the item and appends its ID to the bounded {notif}:dlq list.
// When workerID is empty (e.g., reconciler expiry), only tracking cleanup and DLQ insertion run.
func (q *Queue) DeadLetter(ctx context.Context, workerID string, id uuid.UUID) error {
	if q.rdb == nil {
		return errors.New("redis client is nil")
	}
	keys := []string{MsgHashKey, DLQListKey}
	if workerID != "" {
		keys = append(keys, InflightKey(workerID))
	}
	if err := deadletterScript.Run(ctx, q.rdb, keys, id.String(), q.dlqMaxLen).Err(); err != nil {
		return fmt.Errorf("deadlettering notification %s: %w", id, err)
	}
	return nil
}

// RequeueInflight moves every item remaining in workerID's inflight list back to the front
// of its channel/priority queue while preserving FIFO ordering.
func (q *Queue) RequeueInflight(ctx context.Context, workerID string) (int64, error) {
	if q.rdb == nil {
		return 0, errors.New("redis client is nil")
	}
	n, err := requeueScript.Run(ctx, q.rdb, []string{MsgHashKey, InflightKey(workerID)}, QueuePrefix).Int64()
	if err != nil {
		return 0, fmt.Errorf("requeueing inflight for worker %s: %w", workerID, err)
	}
	return n, nil
}

// InflightIDs lists the IDs currently parked in workerID's inflight list.
func (q *Queue) InflightIDs(ctx context.Context, workerID string) ([]uuid.UUID, error) {
	if q.rdb == nil {
		return nil, errors.New("redis client is nil")
	}
	raw, err := q.rdb.LRange(ctx, InflightKey(workerID), 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("listing inflight for worker %s: %w", workerID, err)
	}
	ids := make([]uuid.UUID, 0, len(raw))
	for _, s := range raw {
		id, parseErr := uuid.Parse(s)
		if parseErr != nil {
			continue
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// RequeueInflightIDs moves only the given IDs from workerID's inflight list back to their queues.
// IDs no longer present in the inflight list (acked meanwhile) are skipped.
func (q *Queue) RequeueInflightIDs(ctx context.Context, workerID string, ids []uuid.UUID) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	if q.rdb == nil {
		return 0, errors.New("redis client is nil")
	}
	args := make([]any, 0, len(ids)+1)
	args = append(args, QueuePrefix)
	for _, id := range ids {
		args = append(args, id.String())
	}
	n, err := requeueIDsScript.Run(ctx, q.rdb, []string{MsgHashKey, InflightKey(workerID)}, args...).Int64()
	if err != nil {
		return 0, fmt.Errorf("requeueing leftover inflight ids for worker %s: %w", workerID, err)
	}
	return n, nil
}

// MoveDue atomically promotes up to limit delayed notification IDs with score <= nowMs to their priority queues.
func (q *Queue) MoveDue(ctx context.Context, now time.Time, limit int) (int64, error) {
	if q.rdb == nil {
		return 0, errors.New("redis client is nil")
	}
	if limit <= 0 {
		limit = 500
	}
	nowMs := strconv.FormatInt(now.UTC().UnixMilli(), 10)
	n, err := moveDueScript.Run(ctx, q.rdb, []string{MsgHashKey, DelayedZSetKey}, nowMs, limit, QueuePrefix).Int64()
	if err != nil {
		return 0, fmt.Errorf("moving due notifications: %w", err)
	}
	return n, nil
}

// Heartbeat refreshes workerID's liveness key with expiration ttl.
func (q *Queue) Heartbeat(ctx context.Context, workerID string, ttl time.Duration) error {
	if q.rdb == nil {
		return errors.New("redis client is nil")
	}
	if err := q.rdb.Set(ctx, HeartbeatKey(workerID), strconv.FormatInt(time.Now().UTC().Unix(), 10), ttl).Err(); err != nil {
		return fmt.Errorf("setting worker heartbeat: %w", err)
	}
	return nil
}

// DeleteHeartbeat removes workerID's heartbeat key cleanly on graceful shutdown.
func (q *Queue) DeleteHeartbeat(ctx context.Context, workerID string) error {
	if q.rdb == nil {
		return errors.New("redis client is nil")
	}
	if err := q.rdb.Del(ctx, HeartbeatKey(workerID)).Err(); err != nil {
		return fmt.Errorf("deleting worker heartbeat: %w", err)
	}
	return nil
}

// IsTracked checks which notification IDs currently have tracking metadata in {notif}:msg via pipelined HEXISTS.
func (q *Queue) IsTracked(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]bool, error) {
	out := make(map[uuid.UUID]bool, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	if q.rdb == nil {
		return nil, errors.New("redis client is nil")
	}

	pipe := q.rdb.Pipeline()
	cmds := make([]*redis.BoolCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.HExists(ctx, MsgHashKey, id.String())
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("checking tracked ids: %w", err)
	}
	for i, id := range ids {
		out[id] = cmds[i].Val()
	}
	return out, nil
}

// Depths measures live Redis queue depths across channels/priorities, delayed ZSET, DLQ, and inflight lists.
func (q *Queue) Depths(ctx context.Context) (MetricsDepths, error) {
	if q.rdb == nil {
		return MetricsDepths{}, errors.New("redis client is nil")
	}

	channels := []domain.Channel{domain.ChannelSMS, domain.ChannelEmail, domain.ChannelPush}
	priorities := []domain.Priority{domain.PriorityHigh, domain.PriorityNormal, domain.PriorityLow}

	depths := MetricsDepths{
		ByChannel: make(map[domain.Channel]map[domain.Priority]int64, len(channels)),
	}
	pipe := q.rdb.Pipeline()
	type qCmd struct {
		ch  domain.Channel
		pr  domain.Priority
		cmd *redis.IntCmd
	}
	var queueCmds []qCmd
	for _, ch := range channels {
		depths.ByChannel[ch] = make(map[domain.Priority]int64, len(priorities))
		for _, pr := range priorities {
			queueCmds = append(queueCmds, qCmd{
				ch:  ch,
				pr:  pr,
				cmd: pipe.LLen(ctx, Key(ch, pr)),
			})
		}
	}
	delayedCmd := pipe.ZCard(ctx, DelayedZSetKey)
	dlqCmd := pipe.LLen(ctx, DLQListKey)
	if _, err := pipe.Exec(ctx); err != nil {
		return MetricsDepths{}, fmt.Errorf("measuring queue depths: %w", err)
	}

	for _, qc := range queueCmds {
		depths.ByChannel[qc.ch][qc.pr] = qc.cmd.Val()
	}
	depths.Delayed = delayedCmd.Val()
	depths.DLQ = dlqCmd.Val()

	inflightKeys, err := q.scanKeys(ctx, InflightScanPattern)
	if err != nil {
		return MetricsDepths{}, fmt.Errorf("scanning inflight lists: %w", err)
	}
	if len(inflightKeys) > 0 {
		inPipe := q.rdb.Pipeline()
		lenCmds := make([]*redis.IntCmd, len(inflightKeys))
		for i, k := range inflightKeys {
			lenCmds[i] = inPipe.LLen(ctx, k)
		}
		if _, err := inPipe.Exec(ctx); err != nil {
			return MetricsDepths{}, fmt.Errorf("measuring inflight depths: %w", err)
		}
		for _, cmd := range lenCmds {
			depths.Inflight += cmd.Val()
		}
	}

	return depths, nil
}

// AcquireReconcilerLock attempts to take the singleton distributed reconciler lease for ttl.
func (q *Queue) AcquireReconcilerLock(ctx context.Context, owner string, ttl time.Duration) (bool, error) {
	if q.rdb == nil {
		return false, errors.New("redis client is nil")
	}
	if owner == "" {
		return false, errors.New("lock owner must not be empty")
	}
	ok, err := q.rdb.SetNX(ctx, ReconcilerLockKey, owner, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("acquiring reconciler lock: %w", err)
	}
	return ok, nil
}

// ReleaseReconcilerLock releases the reconciler lease only if owner still holds the token.
func (q *Queue) ReleaseReconcilerLock(ctx context.Context, owner string) (bool, error) {
	if q.rdb == nil {
		return false, errors.New("redis client is nil")
	}
	if owner == "" {
		return false, errors.New("lock owner must not be empty")
	}
	res, err := unlockScript.Run(ctx, q.rdb, []string{ReconcilerLockKey}, owner).Int64()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return false, nil
		}
		return false, fmt.Errorf("releasing reconciler lock: %w", err)
	}
	return res == 1, nil
}

// ExtendReconcilerLock resets the reconciler lease TTL if owner still holds it. false means the lease
// expired and possibly belongs to another replica now: the caller must stop its cycle.
func (q *Queue) ExtendReconcilerLock(ctx context.Context, owner string, ttl time.Duration) (bool, error) {
	if q.rdb == nil {
		return false, errors.New("redis client is nil")
	}
	if owner == "" {
		return false, errors.New("lock owner must not be empty")
	}
	res, err := extendLockScript.Run(ctx, q.rdb, []string{ReconcilerLockKey}, owner, ttl.Milliseconds()).Int64()
	if err != nil {
		return false, fmt.Errorf("extending reconciler lock: %w", err)
	}
	return res == 1, nil
}

// ScanInflightWorkerIDs returns sorted worker IDs that currently have an inflight list key in Redis.
func (q *Queue) ScanInflightWorkerIDs(ctx context.Context) ([]string, error) {
	if q.rdb == nil {
		return nil, errors.New("redis client is nil")
	}
	keys, err := q.scanKeys(ctx, InflightScanPattern)
	if err != nil {
		return nil, fmt.Errorf("scanning inflight worker keys: %w", err)
	}
	seen := make(map[string]struct{}, len(keys))
	ids := make([]string, 0, len(keys))
	for _, k := range keys {
		id := strings.TrimPrefix(k, InflightPrefix)
		if id == "" || id == k {
			continue
		}
		if _, exists := seen[id]; !exists {
			seen[id] = struct{}{}
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids, nil
}

// HasHeartbeats reports which worker IDs currently have an active heartbeat key in Redis.
func (q *Queue) HasHeartbeats(ctx context.Context, workerIDs []string) (map[string]bool, error) {
	out := make(map[string]bool, len(workerIDs))
	if len(workerIDs) == 0 {
		return out, nil
	}
	if q.rdb == nil {
		return nil, errors.New("redis client is nil")
	}

	pipe := q.rdb.Pipeline()
	cmds := make([]*redis.IntCmd, len(workerIDs))
	for i, id := range workerIDs {
		cmds[i] = pipe.Exists(ctx, HeartbeatKey(id))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("checking worker heartbeats: %w", err)
	}
	for i, id := range workerIDs {
		out[id] = cmds[i].Val() > 0
	}
	return out, nil
}

// DeleteInflightKey deletes workerID's inflight list key after its entries have been requeued.
func (q *Queue) DeleteInflightKey(ctx context.Context, workerID string) error {
	if q.rdb == nil {
		return errors.New("redis client is nil")
	}
	if err := q.rdb.Del(ctx, InflightKey(workerID)).Err(); err != nil {
		return fmt.Errorf("deleting inflight key for %s: %w", workerID, err)
	}
	return nil
}

func (q *Queue) scanKeys(ctx context.Context, pattern string) ([]string, error) {
	var keys []string
	var cursor uint64
	for {
		batch, next, err := q.rdb.Scan(ctx, cursor, pattern, 200).Result()
		if err != nil {
			return nil, err
		}
		keys = append(keys, batch...)
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return keys, nil
}

func (q *Queue) nextSlot(ch domain.Channel) int {
	q.slotMu.Lock()
	ctr, ok := q.slotByCh[ch]
	if !ok {
		ctr = &atomic.Uint64{}
		q.slotByCh[ch] = ctr
	}
	q.slotMu.Unlock()
	val := ctr.Add(1) - 1
	return int(val % uint64(len(weightedSlotSchedule)))
}

func isNoScriptError(err error, cmds []*redis.Cmd) bool {
	if err != nil && strings.Contains(err.Error(), "NOSCRIPT") {
		return true
	}
	for _, cmd := range cmds {
		if cmd.Err() != nil && strings.Contains(cmd.Err().Error(), "NOSCRIPT") {
			return true
		}
	}
	return false
}
