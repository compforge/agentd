// Package connector owns the Agentlet HTTP data plane.
package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/compforge/agentd/internal/executionapi"
)

const maxResponseBody = 2 << 20

type Config struct {
	RequestTimeout        time.Duration
	DialTimeout           time.Duration
	ResponseHeaderTimeout time.Duration
	IdleConnTimeout       time.Duration
	MaxIdleConns          int
	MaxIdleConnsPerHost   int
}

type Target struct {
	Endpoint string
	Work     executionapi.WorkSpec
}

type EventTarget struct {
	Endpoint string
	WorkerID string
}

type Client struct {
	requestTimeout time.Duration
	client         *http.Client
	streamClient   *http.Client
	transport      *http.Transport
}

func New(config Config) (*Client, error) {
	if config.RequestTimeout <= 0 || config.DialTimeout <= 0 || config.ResponseHeaderTimeout <= 0 ||
		config.IdleConnTimeout <= 0 {
		return nil, fmt.Errorf("create Agentlet Connector: timeouts must be positive")
	}
	if config.MaxIdleConns <= 0 || config.MaxIdleConnsPerHost <= 0 {
		return nil, fmt.Errorf("create Agentlet Connector: connection pool limits must be positive")
	}
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout: config.DialTimeout, KeepAlive: 30 * time.Second,
		}).DialContext,
		ResponseHeaderTimeout: config.ResponseHeaderTimeout,
		IdleConnTimeout:       config.IdleConnTimeout,
		MaxIdleConns:          config.MaxIdleConns,
		MaxIdleConnsPerHost:   config.MaxIdleConnsPerHost,
		ForceAttemptHTTP2:     true,
	}
	return &Client{
		requestTimeout: config.RequestTimeout,
		client:         &http.Client{Transport: transport, Timeout: config.RequestTimeout},
		streamClient:   &http.Client{Transport: transport},
		transport:      transport,
	}, nil
}

func (c *Client) CloseIdleConnections() {
	c.transport.CloseIdleConnections()
}

// Ensure installs the current control-plane snapshot before any data-plane
// request. Repeating it for the same Assignment is idempotent.
func (c *Client) Ensure(ctx context.Context, target Target) error {
	body, err := json.Marshal(target.Work)
	if err != nil {
		return fmt.Errorf("encode WorkSpec for Session %q: %w", target.Work.Session.ID, err)
	}
	response, err := c.do(ctx, target, http.MethodPut, sessionPath(target.Work.Session.ID), "", body, nil, false)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return responseError("ensure Agentlet Work", response)
	}
	_, err = io.Copy(io.Discard, io.LimitReader(response.Body, maxResponseBody+1))
	return err
}

func (c *Client) SessionState(ctx context.Context, target Target) (executionapi.SessionState, error) {
	response, err := c.do(
		ctx, target, http.MethodGet, sessionPath(target.Work.Session.ID)+"/state", "", nil, nil, false,
	)
	if err != nil {
		return executionapi.SessionState{}, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return executionapi.SessionState{}, responseError("read Agentlet Session state", response)
	}
	var state executionapi.SessionState
	decoder := json.NewDecoder(io.LimitReader(response.Body, maxResponseBody+1))
	if err := decoder.Decode(&state); err != nil {
		return executionapi.SessionState{}, fmt.Errorf("decode Agentlet Session state: %w", err)
	}
	return state, nil
}

// Forward sends an already validated public Event request to Agentlet. The
// caller owns and must close the returned response body.
func (c *Client) Forward(
	ctx context.Context,
	target Target,
	method string,
	path string,
	rawQuery string,
	body []byte,
	headers http.Header,
	stream bool,
) (*http.Response, error) {
	return c.do(ctx, target, method, path, rawQuery, body, headers, stream)
}

// ForwardEventRead forwards a location-independent Event read without an
// Assignment fence. WorkerID identifies the selected data-plane endpoint.
func (c *Client) ForwardEventRead(
	ctx context.Context,
	target EventTarget,
	method string,
	path string,
	rawQuery string,
	headers http.Header,
	stream bool,
) (*http.Response, error) {
	return c.doRequest(ctx, target.Endpoint, target.WorkerID, "", method, path, rawQuery, nil, headers, stream)
}

func (c *Client) do(
	ctx context.Context,
	target Target,
	method string,
	path string,
	rawQuery string,
	body []byte,
	headers http.Header,
	stream bool,
) (*http.Response, error) {
	return c.doRequest(
		ctx, target.Endpoint, target.Work.WorkerID, target.Work.AssignmentID,
		method, path, rawQuery, body, headers, stream,
	)
}

func (c *Client) doRequest(
	ctx context.Context,
	endpointBase string,
	workerID string,
	assignmentID string,
	method string,
	path string,
	rawQuery string,
	body []byte,
	headers http.Header,
	stream bool,
) (*http.Response, error) {
	endpoint, err := requestURL(endpointBase, path, rawQuery)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create Agentlet request: %w", err)
	}
	for name, values := range headers {
		for _, value := range values {
			request.Header.Add(name, value)
		}
	}
	if assignmentID != "" {
		request.Header.Set(executionapi.AssignmentHeader, assignmentID)
	}
	if workerID != "" {
		request.Header.Set(executionapi.WorkerHeader, workerID)
	}
	if len(body) > 0 && request.Header.Get("Content-Type") == "" {
		request.Header.Set("Content-Type", "application/json")
	}
	client := c.client
	if stream {
		client = c.streamClient
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("call Agentlet %s %s: %w", method, path, err)
	}
	return response, nil
}

func requestURL(endpoint, requestPath, rawQuery string) (string, error) {
	base, err := url.Parse(endpoint)
	if err != nil || base.Scheme == "" || base.Host == "" {
		return "", fmt.Errorf("invalid Agentlet endpoint %q", endpoint)
	}
	base.Path = strings.TrimRight(base.Path, "/") + requestPath
	base.RawQuery = rawQuery
	return base.String(), nil
}

func sessionPath(sessionID string) string {
	return "/internal/v1/sessions/" + url.PathEscape(sessionID)
}

func responseError(operation string, response *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	message := strings.TrimSpace(string(body))
	if message == "" {
		message = http.StatusText(response.StatusCode)
	}
	return fmt.Errorf("%s: Agentlet returned %d: %s", operation, response.StatusCode, message)
}
