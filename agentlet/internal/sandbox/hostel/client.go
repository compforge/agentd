package hostel

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"mime/multipart"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/compforge/agentd/agentlet/internal/sandbox/engine"
)

const bedHeader = "X-Hostel-Bed"

type Client struct {
	baseURL    *url.URL
	httpClient *http.Client

	mu      sync.Mutex
	bedLock map[string]*sync.Mutex
}

func NewClient(rawURL string, timeout time.Duration) (*Client, error) {
	if timeout <= 0 {
		return nil, errors.New("create hostel client: timeout must be positive")
	}
	baseURL, err := url.Parse(rawURL)
	if err != nil || baseURL.Scheme == "" || baseURL.Host == "" {
		return nil, fmt.Errorf("create hostel client: invalid base URL %q", rawURL)
	}
	return &Client{
		baseURL:    baseURL,
		httpClient: &http.Client{Timeout: timeout},
		bedLock:    make(map[string]*sync.Mutex),
	}, nil
}

func (c *Client) EnsureBed(ctx context.Context, bedID string) error {
	lock := c.lockForBed(bedID)
	lock.Lock()
	defer lock.Unlock()

	body, _ := json.Marshal(map[string]string{"id": bedID})
	request, err := c.request(ctx, http.MethodPost, "/v1/beds", "", bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("create hostel bed %q: %w", bedID, err)
	}
	ready, err := bedReady(response)
	response.Body.Close()
	if err != nil {
		return fmt.Errorf("create hostel bed %q: %w", bedID, err)
	}
	if ready {
		return nil
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for hostel bed %q: %w", bedID, ctx.Err())
		case <-ticker.C:
			request, err := c.request(ctx, http.MethodGet, "/v1/beds/"+url.PathEscape(bedID), "", nil)
			if err != nil {
				return err
			}
			response, err := c.httpClient.Do(request)
			if err != nil {
				return fmt.Errorf("get hostel bed %q: %w", bedID, err)
			}
			ready, err := bedReady(response)
			response.Body.Close()
			if err != nil {
				return fmt.Errorf("get hostel bed %q: %w", bedID, err)
			}
			if ready {
				return nil
			}
		}
	}
}

func (c *Client) Run(ctx context.Context, bedID string, command engine.Command) (engine.CommandResult, error) {
	if err := c.EnsureBed(ctx, bedID); err != nil {
		return engine.CommandResult{}, err
	}
	payload := map[string]any{"command": command.Command, "cwd": command.Cwd}
	if command.Timeout > 0 {
		payload["timeout"] = command.Timeout.Milliseconds()
	}
	body, _ := json.Marshal(payload)
	request, err := c.request(ctx, http.MethodPost, "/command", bedID, bytes.NewReader(body))
	if err != nil {
		return engine.CommandResult{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.httpClient.Do(request)
	if err != nil {
		return engine.CommandResult{}, fmt.Errorf("run hostel command: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return engine.CommandResult{}, responseError("run hostel command", response)
	}

	var result engine.CommandResult
	scanner := bufio.NewScanner(response.Body)
	scanner.Buffer(nil, 4<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var event struct {
			Type     string `json:"type"`
			Text     string `json:"text"`
			ExitCode *int   `json:"exit_code"`
			Error    string `json:"error"`
			Result   *struct {
				Process struct {
					Kind     string `json:"kind"`
					ExitCode *int   `json:"exit_code"`
					Error    string `json:"error"`
				} `json:"process"`
				Cause string `json:"termination_cause"`
			} `json:"result"`
		}
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return engine.CommandResult{}, fmt.Errorf("decode hostel command event: %w", err)
		}
		switch event.Type {
		case "stdout", "stderr":
			result.Output += event.Text
		case "execution_end":
			if event.Result != nil {
				result.Cause = event.Result.Cause
				if event.Result.Process.ExitCode != nil {
					result.ExitCode = *event.Result.Process.ExitCode
				}
				if event.Result.Process.Error != "" {
					result.Error = event.Result.Process.Error
				}
			}
		case "execution_complete":
			if event.ExitCode != nil {
				result.ExitCode = *event.ExitCode
				result.Cause = "exited"
			}
			result.Error = event.Error
		}
	}
	if err := scanner.Err(); err != nil {
		return engine.CommandResult{}, fmt.Errorf("read hostel command stream: %w", err)
	}
	return result, nil
}

func (c *Client) Stat(ctx context.Context, bedID, filePath string) (engine.FileInfo, error) {
	if err := c.EnsureBed(ctx, bedID); err != nil {
		return engine.FileInfo{}, err
	}
	query := url.Values{"path": []string{filePath}}
	request, err := c.request(ctx, http.MethodGet, "/files/info?"+query.Encode(), bedID, nil)
	if err != nil {
		return engine.FileInfo{}, err
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		return engine.FileInfo{}, fmt.Errorf("stat hostel file %q: %w", filePath, err)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return engine.FileInfo{}, fs.ErrNotExist
	}
	if response.StatusCode != http.StatusOK {
		return engine.FileInfo{}, responseError("stat hostel file", response)
	}
	var values map[string]fileInfo
	if err := json.NewDecoder(response.Body).Decode(&values); err != nil {
		return engine.FileInfo{}, fmt.Errorf("decode hostel file info: %w", err)
	}
	value, ok := values[filePath]
	if !ok {
		return engine.FileInfo{}, fs.ErrNotExist
	}
	return value.engineFileInfo(), nil
}

func (c *Client) ReadFile(ctx context.Context, bedID, filePath string) ([]byte, error) {
	if err := c.EnsureBed(ctx, bedID); err != nil {
		return nil, err
	}
	query := url.Values{"path": []string{filePath}}
	request, err := c.request(ctx, http.MethodGet, "/files/download?"+query.Encode(), bedID, nil)
	if err != nil {
		return nil, err
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("read hostel file %q: %w", filePath, err)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return nil, fs.ErrNotExist
	}
	if response.StatusCode != http.StatusOK {
		return nil, responseError("read hostel file", response)
	}
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("read hostel file body: %w", err)
	}
	return data, nil
}

func (c *Client) ReadDir(ctx context.Context, bedID, directory string) ([]engine.DirEntry, error) {
	if err := c.EnsureBed(ctx, bedID); err != nil {
		return nil, err
	}
	query := url.Values{"path": []string{directory}, "depth": []string{"1"}}
	request, err := c.request(ctx, http.MethodGet, "/directories/list?"+query.Encode(), bedID, nil)
	if err != nil {
		return nil, err
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("list hostel directory %q: %w", directory, err)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return nil, fs.ErrNotExist
	}
	if response.StatusCode != http.StatusOK {
		return nil, responseError("list hostel directory", response)
	}
	var values []fileInfo
	if err := json.NewDecoder(response.Body).Decode(&values); err != nil {
		return nil, fmt.Errorf("decode hostel directory listing: %w", err)
	}
	entries := make([]engine.DirEntry, 0, len(values))
	for _, value := range values {
		if path.Clean(value.Path) == path.Clean(directory) {
			continue
		}
		entries = append(entries, engine.DirEntry{Name: path.Base(value.Path), IsDir: value.Type == "directory"})
	}
	return entries, nil
}

func (c *Client) WriteFile(ctx context.Context, bedID, filePath string, data []byte, mode fs.FileMode) error {
	if err := c.EnsureBed(ctx, bedID); err != nil {
		return err
	}
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	metadata, _ := json.Marshal(map[string]any{"path": filePath, "mode": mode.Perm()})
	if err := writer.WriteField("metadata", string(metadata)); err != nil {
		return fmt.Errorf("encode hostel upload metadata: %w", err)
	}
	part, err := writer.CreateFormFile("file", path.Base(filePath))
	if err != nil {
		return fmt.Errorf("create hostel upload part: %w", err)
	}
	if _, err := part.Write(data); err != nil {
		return fmt.Errorf("write hostel upload part: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("finish hostel upload: %w", err)
	}
	request, err := c.request(ctx, http.MethodPost, "/files/upload", bedID, &body)
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", writer.FormDataContentType())
	response, err := c.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("write hostel file %q: %w", filePath, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return responseError("write hostel file", response)
	}
	return nil
}

func (c *Client) MkdirAll(ctx context.Context, bedID, directory string, _ fs.FileMode) error {
	if err := c.EnsureBed(ctx, bedID); err != nil {
		return err
	}
	query := url.Values{"path": []string{directory}}
	request, err := c.request(ctx, http.MethodPost, "/directories?"+query.Encode(), bedID, nil)
	if err != nil {
		return err
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("create hostel directory %q: %w", directory, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return responseError("create hostel directory", response)
	}
	return nil
}

type fileInfo struct {
	Path       string    `json:"path"`
	Type       string    `json:"type"`
	Size       int64     `json:"size"`
	ModifiedAt time.Time `json:"modified_at"`
	Mode       int       `json:"mode"`
}

func (f fileInfo) engineFileInfo() engine.FileInfo {
	return engine.FileInfo{
		Name:       path.Base(f.Path),
		Size:       f.Size,
		Mode:       fs.FileMode(f.Mode),
		ModifiedAt: f.ModifiedAt,
		IsDir:      f.Type == "directory",
		Version:    f.ModifiedAt.UTC().Format(time.RFC3339Nano) + ":" + strconv.FormatInt(f.Size, 10),
	}
}

func (c *Client) request(ctx context.Context, method, requestPath, bedID string, body io.Reader) (*http.Request, error) {
	target := c.baseURL.ResolveReference(&url.URL{Path: requestPath})
	if strings.Contains(requestPath, "?") {
		parts := strings.SplitN(requestPath, "?", 2)
		target = c.baseURL.ResolveReference(&url.URL{Path: parts[0], RawQuery: parts[1]})
	}
	request, err := http.NewRequestWithContext(ctx, method, target.String(), body)
	if err != nil {
		return nil, fmt.Errorf("create hostel request: %w", err)
	}
	if bedID != "" {
		request.Header.Set(bedHeader, bedID)
	}
	return request, nil
}

func (c *Client) lockForBed(bedID string) *sync.Mutex {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.bedLock[bedID] == nil {
		c.bedLock[bedID] = &sync.Mutex{}
	}
	return c.bedLock[bedID]
}

func bedReady(response *http.Response) (bool, error) {
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusAccepted {
		return false, responseError("inspect hostel bed", response)
	}
	var payload struct {
		Readiness struct {
			Ready bool `json:"ready"`
		} `json:"readiness"`
		Ready bool   `json:"ready"`
		State string `json:"state"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return false, fmt.Errorf("decode hostel bed: %w", err)
	}
	return payload.Ready || payload.Readiness.Ready || payload.State == "active", nil
}

func responseError(operation string, response *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(response.Body, 16<<10))
	return fmt.Errorf("%s: hostel returned %s: %s", operation, response.Status, strings.TrimSpace(string(body)))
}
