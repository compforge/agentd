package hostel

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/compforge/agentd/agentlet/internal/sandbox/engine"
)

func TestCommandTerminalOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name, terminal, kind, cause, detail string
		exit, signal                        *int
		core                                bool
	}{
		{name: "success", terminal: `{"type":"execution_end","result":{"process":{"kind":"exited","exit_code":0},"termination_cause":"natural"}}`, kind: "exited", cause: "natural", exit: intValue(0)},
		{name: "nonzero", terminal: `{"type":"execution_end","result":{"process":{"kind":"exited","exit_code":7},"termination_cause":"natural"}}`, kind: "exited", cause: "natural", exit: intValue(7)},
		{name: "signal", terminal: `{"type":"execution_end","result":{"process":{"kind":"signaled","signal":11,"core_dumped":true},"termination_cause":"external_signal"}}`, kind: "signaled", cause: "external_signal", signal: intValue(11), core: true},
		{name: "timeout", terminal: `{"type":"execution_end","result":{"process":{"kind":"signaled","signal":9},"termination_cause":"timeout"}}`, kind: "signaled", cause: "timeout", signal: intValue(9)},
		{name: "canceled exit zero", terminal: `{"type":"execution_end","result":{"process":{"kind":"exited","exit_code":0},"termination_cause":"client_canceled"}}`, kind: "exited", cause: "client_canceled", exit: intValue(0)},
		{name: "lost", terminal: `{"type":"execution_end","result":{"process":{"kind":"lost","error":"executor lost"},"termination_cause":"executor_lost"}}`, kind: "lost", cause: "executor_lost", detail: "executor lost"},
		{name: "preparation", terminal: `{"type":"execution_end","result":{"process":null,"error":"prepare workspace failed","termination_cause":"preparation_failed"}}`, cause: "preparation_failed", detail: "prepare workspace failed"},
		{name: "legacy success", terminal: `{"type":"execution_complete","exit_code":0}`, kind: "exited", cause: "exited", exit: intValue(0)},
		{name: "legacy failure", terminal: `{"type":"execution_complete","exit_code":2,"error":"failed"}`, kind: "exited", cause: "exited", exit: intValue(2), detail: "failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream := `{"type":"execution_start","execution_id":"exec-1"}` + "\n" + `{"type":"stdout","text":"out"}` + "\n" + `{"type":"stderr","text":"err"}` + "\n" + tc.terminal + "\n"
			result, err := readCommandResult(strings.NewReader(stream))
			if err != nil {
				t.Fatal(err)
			}
			if result.Output != "outerr" || result.ProcessKind != tc.kind || result.Cause != tc.cause || result.Error != tc.detail || result.CoreDumped != tc.core || !sameInt(result.ExitCode, tc.exit) || !sameInt(result.Signal, tc.signal) {
				t.Fatalf("result=%+v", result)
			}
			raw, err := json.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			if tc.exit == nil && strings.Contains(string(raw), "exit_code") {
				t.Fatalf("invented exit code: %s", raw)
			}
		})
	}
}

func TestIncompleteOrMalformedTerminalIsUnknown(t *testing.T) {
	for _, terminal := range []string{
		"", `{"type":"ping"}`, `{"type":"execution_end"}`, `{"type":"execution_end","result":{}}`,
		`{"type":"execution_end","result":{"process":{"kind":"exited"},"termination_cause":"natural"}}`,
		`{"type":"execution_end","result":{"process":{"kind":"signaled"},"termination_cause":"timeout"}}`,
		`{"type":"execution_end","result":{"process":{"kind":"signaled","signal":0},"termination_cause":"timeout"}}`,
		`{"type":"execution_end","result":{"process":null,"termination_cause":"natural"}}`,
		`{"type":"execution_end","result":{"process":{"kind":"exited","exit_code":0},"termination_cause":"preparation_failed"}}`,
		`{"type":"execution_end","result":{"process":{"kind":"unrecognized"},"termination_cause":"natural"}}`,
		`{"type":"execution_complete"}`, `{"type":"execution_complete","error":"unknown"}`, `{"type":`,
	} {
		t.Run(terminal, func(t *testing.T) {
			result, err := readCommandResult(strings.NewReader(`{"type":"stdout","text":"partial"}` + "\n" + terminal))
			if !errors.Is(err, engine.ErrExecutionUnknown) {
				t.Fatalf("error=%v", err)
			}
			if result.ExitCode != nil || result.Output != "partial" {
				t.Fatalf("unknown result=%+v", result)
			}
		})
	}
}

type failureReader struct{ err error }

func (r failureReader) Read([]byte) (int, error) { return 0, r.err }

func TestStreamFailurePreservesCauseAndKnownTerminalWins(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded, io.ErrUnexpectedEOF} {
		result, err := readCommandResult(io.MultiReader(strings.NewReader(`{"type":"stdout","text":"partial"}`+"\n"), failureReader{cause}))
		if !errors.Is(err, engine.ErrExecutionUnknown) || !errors.Is(err, cause) || result.Output != "partial" {
			t.Fatalf("result=%+v error=%v", result, err)
		}
	}
	result, err := readCommandResult(io.MultiReader(strings.NewReader(`{"type":"execution_end","result":{"process":{"kind":"exited","exit_code":4},"termination_cause":"natural"}}`+"\n"), failureReader{io.ErrUnexpectedEOF}))
	if err != nil || result.ExitCode == nil || *result.ExitCode != 4 {
		t.Fatalf("transport overrode known terminal: %+v %v", result, err)
	}
}

func TestCommandEOFIsNotSuccessAndDoesNotResubmit(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/beds" {
			_, _ = io.WriteString(w, `{"state":"idle"}`)
			return
		}
		calls.Add(1)
		_, _ = io.WriteString(w, `{"type":"stdout","text":"side effect may have completed"}`+"\n")
	}))
	defer server.Close()
	client, err := NewClient(server.URL, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.Run(context.Background(), "bed", engine.Command{Command: "do-work"})
	if !errors.Is(err, engine.ErrExecutionUnknown) || !errors.Is(err, io.ErrUnexpectedEOF) || result.ExitCode != nil {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("command submitted %d times", calls.Load())
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestSubmissionTransportFailureIsUnknown(t *testing.T) {
	client, err := NewClient("http://hostel.test", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	client.httpClient.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/v1/beds" {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"state":"idle"}`)), Header: make(http.Header)}, nil
		}
		calls++
		return nil, io.ErrUnexpectedEOF
	})
	_, err = client.Run(context.Background(), "bed", engine.Command{Command: "do-work"})
	if !errors.Is(err, engine.ErrExecutionUnknown) || !errors.Is(err, io.ErrUnexpectedEOF) || calls != 1 {
		t.Fatalf("calls=%d error=%v", calls, err)
	}
}

func intValue(v int) *int    { return &v }
func sameInt(a, b *int) bool { return a == nil && b == nil || a != nil && b != nil && *a == *b }
