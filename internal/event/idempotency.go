package event

import (
	"context"
	"crypto/sha256"
	"fmt"
)

const requestDigestExtension = "managed.request_digest"

// UserMessageRequestID maps an opaque key to a globally unique Ledger event ID.
// The Session is deliberately excluded: the same Event key cannot be reused in
// another Session. Only a digest of the raw key enters the Ledger.
func UserMessageRequestID(key string) string {
	digest := sha256.Sum256([]byte("agentd/user.message/" + key))
	// UUIDv8 carries the truncated SHA-256 in the Ledger's UUID-sized ID column.
	digest[6] = (digest[6] & 0x0f) | 0x80
	digest[8] = (digest[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", digest[:4], digest[4:6], digest[6:8], digest[8:10], digest[10:16])
}

// FindUserMessageRequest returns the immutable accepted input, not its current
// public projection (whose processed_at may since have changed).
func (l *Log) FindUserMessageRequest(ctx context.Context, sessionID, requestID string) (ManagedEvent, string, bool, error) {
	lane, exists, err := l.store.FindLane(ctx, sessionID, managedEventRun, managedEventLane)
	if err != nil || !exists {
		return nil, "", false, err
	}
	for stored, err := range l.store.LoadLane(ctx, lane.ID, 0) {
		if err != nil {
			return nil, "", false, err
		}
		if stored.ID != requestID {
			continue
		}
		value, err := mapEvent(stored.Payload["event"])
		digest, _ := stored.Extensions[requestDigestExtension].(string)
		return value, digest, true, err
	}
	return nil, "", false, nil
}

// AppendUserMessageRequest persists request identity with the original input.
// +spec=`Keyed user.message retries reuse one immutable Event; request metadata never leaks into public Event responses`
// +rule=`The caller holds the Session write lock and commits this append in the same transaction as resource validation`
// +link=agentd/docs/agentd.md#资源请求与一致性
func (l *Log) AppendUserMessageRequest(ctx context.Context, sessionID, requestID, digest string, content any) (ManagedEvent, error) {
	actor, err := l.store.EnsureActor(ctx, l.ingressActor)
	if err != nil {
		return nil, err
	}
	recorder, err := l.openRecorder(ctx, sessionID, actor)
	if err != nil {
		return nil, err
	}
	value := ManagedEvent{"id": "event_" + requestID, "type": "user.message", "content": content, "processed_at": nil}
	proposed := proposeManagedEvents(recorder.Lane().ID, actor, []ManagedEvent{value})[0]
	proposed.ID = requestID
	proposed.Extensions = map[string]any{requestDigestExtension: digest}
	// Both the Event primary key and append identity are stable across replicas.
	// A concurrent claim in another Session must conflict, never append twice.
	if _, err := recorder.Append(ctx, requestID, proposed); err != nil {
		return nil, err
	}
	return value, nil
}
