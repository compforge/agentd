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

type Client struct {
	requestTimeout time.Duration
	client         *http.Client
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
	response, err := c.do(ctx, target, http.MethodPut, sessionPath(target.Work.Session.ID), body)
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
	response, err := c.do(ctx, target, http.MethodGet, sessionPath(target.Work.Session.ID)+"/state", nil)
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

func (c *Client) Wake(ctx context.Context, target Target) error {
	return c.sessionAction(ctx, target, "wake")
}

func (c *Client) Interrupt(ctx context.Context, target Target) error {
	return c.sessionAction(ctx, target, "interrupt")
}

func (c *Client) sessionAction(ctx context.Context, target Target, action string) error {
	path := sessionPath(target.Work.Session.ID) + "/" + action
	response, err := c.do(ctx, target, http.MethodPost, path, nil)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return responseError(action+" Agentlet Session", response)
	}
	_, err = io.Copy(io.Discard, io.LimitReader(response.Body, maxResponseBody+1))
	return err
}

func (c *Client) do(
	ctx context.Context,
	target Target,
	method string,
	path string,
	body []byte,
) (*http.Response, error) {
	return c.doRequest(
		ctx, target.Endpoint, target.Work.WorkerID, target.Work.AssignmentID,
		method, path, body,
	)
}

func (c *Client) doRequest(
	ctx context.Context,
	endpointBase string,
	workerID string,
	assignmentID string,
	method string,
	path string,
	body []byte,
) (*http.Response, error) {
	endpoint, err := requestURL(endpointBase, path)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create Agentlet request: %w", err)
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
	response, err := c.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("call Agentlet %s %s: %w", method, path, err)
	}
	return response, nil
}

func requestURL(endpoint, requestPath string) (string, error) {
	base, err := url.Parse(endpoint)
	if err != nil || base.Scheme == "" || base.Host == "" {
		return "", fmt.Errorf("invalid Agentlet endpoint %q", endpoint)
	}
	base.Path = strings.TrimRight(base.Path, "/") + requestPath
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
