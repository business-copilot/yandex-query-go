package yq

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

const (
	MaxRetryForSession = 4
	BackOffFactor      = 0.3
	TimeBetweenRetries = 1000 * time.Millisecond
	DefaultUserAgent   = "Go YQ HTTP SDK"
	DefaultEndpoint    = "https://api.yandex-query.cloud.yandex.net"
	DefaultWebBaseURL  = "https://yq.cloud.yandex.ru"
	DefaultTokenPrefix = "Bearer "
)

type ClientConfig struct {
	Token       string
	Project     string
	UserAgent   string
	Endpoint    string
	WebBaseURL  string
	TokenPrefix string
}

type YQError struct {
	Message string
	Status  string
	Msg     string
	Details interface{}
}

func (e *YQError) Error() string {
	return fmt.Sprintf("%s (Status: %s, Msg: %s)", e.Message, e.Status, e.Msg)
}

// Client is a YQ HTTP API client.
type Client struct {
	config ClientConfig
	client *http.Client
}

// NewClient creates a new YQ HTTP API client.
func NewClient(config ClientConfig) *Client {
	if config.UserAgent == "" {
		config.UserAgent = DefaultUserAgent
	}
	if config.Endpoint == "" {
		config.Endpoint = DefaultEndpoint
	}
	if config.WebBaseURL == "" {
		config.WebBaseURL = DefaultWebBaseURL
	}
	if config.TokenPrefix == "" {
		config.TokenPrefix = DefaultTokenPrefix
	}

	return &Client{
		config: config,
		client: &http.Client{},
	}
}

func (c *Client) buildHeaders(idempotencyKey, requestID string) http.Header {
	headers := http.Header{}
	headers.Set("Authorization", c.config.TokenPrefix+c.config.Token)
	if idempotencyKey != "" {
		headers.Set("Idempotency-Key", idempotencyKey)
	}
	if requestID != "" {
		headers.Set("x-request-id", requestID)
	}
	if c.config.UserAgent != "" {
		headers.Set("User-Agent", c.config.UserAgent)
	}
	return headers
}

func (c *Client) buildParams() map[string]string {
	params := make(map[string]string)
	if c.config.Project != "" {
		params["project"] = c.config.Project
	}
	return params
}

func (c *Client) composeAPIURL(path string) string {
	return c.config.Endpoint + path
}

func (c *Client) composeWebURL(path string) string {
	return c.config.WebBaseURL + path
}

func (c *Client) doRequest(ctx context.Context, method, url string, headers http.Header, body io.Reader) (*http.Response, error) {
	var resp *http.Response
	var err error

	for i := 0; i <= MaxRetryForSession; i++ {
		req, err := http.NewRequestWithContext(ctx, method, url, body)
		if err != nil {
			return nil, err
		}

		req.Header = headers

		resp, err = c.client.Do(req)
		if err == nil {
			return resp, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(TimeBetweenRetries * time.Duration(i+1)):
			// Exponential backoff
		}
	}

	return nil, err
}

func (c *Client) validateHTTPError(resp *http.Response, expectedCode int) error {
	if resp.StatusCode != expectedCode {
		var body map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&body); err == nil {
			return &YQError{
				Message: fmt.Sprintf("Error occurred. http code=%d, status=%v, msg=%v, details=%v",
					resp.StatusCode, body["status"], body["message"], body["details"]),
				Status:  fmt.Sprintf("%v", body["status"]),
				Msg:     fmt.Sprintf("%v", body["message"]),
				Details: body["details"],
			}
		}
		return &YQError{
			Message: fmt.Sprintf("Error occurred: %d", resp.StatusCode),
		}
	}
	return nil
}

// CreateQuery creates a new query.
func (c *Client) CreateQuery(ctx context.Context, queryText, queryType, name, description, idempotencyKey, requestID string) (string, error) {
	body := map[string]string{}
	if queryText != "" {
		body["text"] = queryText
	}
	if queryType != "" {
		body["type"] = queryType
	}
	if name != "" {
		body["name"] = name
	}
	if description != "" {
		body["description"] = description
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return "", err
	}

	headers := c.buildHeaders(idempotencyKey, requestID)
	headers.Set("Content-Type", "application/json")

	resp, err := c.doRequest(ctx, "POST", c.composeAPIURL("/api/fq/v1/queries"), headers, bytes.NewBuffer(jsonBody))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if err := c.validateHTTPError(resp, http.StatusOK); err != nil {
		return "", err
	}

	var result struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}

	return result.ID, nil
}

// GetQueryStatus returns the status of a query.
func (c *Client) GetQueryStatus(ctx context.Context, queryID, requestID string) (string, error) {
	headers := c.buildHeaders("", requestID)
	resp, err := c.doRequest(ctx, "GET", c.composeAPIURL(fmt.Sprintf("/api/fq/v1/queries/%s/status", queryID)), headers, nil)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if err := c.validateHTTPError(resp, http.StatusOK); err != nil {
		return "", err
	}

	var result struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}

	return result.Status, nil
}

// GetQuery returns the details of a query.
func (c *Client) GetQuery(ctx context.Context, queryID, requestID string) (map[string]interface{}, error) {
	headers := c.buildHeaders("", requestID)
	resp, err := c.doRequest(ctx, "GET", c.composeAPIURL(fmt.Sprintf("/api/fq/v1/queries/%s", queryID)), headers, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := c.validateHTTPError(resp, http.StatusOK); err != nil {
		return nil, err
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result, nil
}

// StopQuery stops a query from executing.
func (c *Client) StopQuery(ctx context.Context, queryID, idempotencyKey, requestID string) error {
	headers := c.buildHeaders(idempotencyKey, requestID)
	resp, err := c.doRequest(ctx, "POST", c.composeAPIURL(fmt.Sprintf("/api/fq/v1/queries/%s/stop", queryID)), headers, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return c.validateHTTPError(resp, http.StatusNoContent)
}

// WaitQueryToComplete waits for a query to complete.
func (c *Client) WaitQueryToComplete(ctx context.Context, queryID string, executionTimeout time.Duration, stopOnTimeout bool) (string, error) {
	startTime := time.Now()
	delay := 200 * time.Millisecond

	for {
		if executionTimeout > 0 && time.Since(startTime) > executionTimeout {
			if stopOnTimeout {
				_ = c.StopQuery(ctx, queryID, "", "")
			}
			return "", fmt.Errorf("query %s execution timeout", queryID)
		}

		status, err := c.GetQueryStatus(ctx, queryID, "")
		if err != nil {
			return "", err
		}

		if status != "RUNNING" && status != "PENDING" {
			return status, nil
		}

		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(delay):
			delay *= 2
			if delay > 2*time.Second {
				delay = 2 * time.Second
			}
		}
	}
}

// WaitQueryToSucceed waits for a query to complete successfully.
func (c *Client) WaitQueryToSucceed(ctx context.Context, queryID string, executionTimeout time.Duration, stopOnTimeout bool) (int, error) {
	status, err := c.WaitQueryToComplete(ctx, queryID, executionTimeout, stopOnTimeout)
	if err != nil {
		return 0, err
	}

	query, err := c.GetQuery(ctx, queryID, "")
	if err != nil {
		return 0, err
	}

	if status != "COMPLETED" {
		issues, _ := query["issues"].([]interface{})
		return 0, fmt.Errorf("query %s failed with issues=%v", queryID, issues)
	}

	resultSets, ok := query["result_sets"].([]interface{})
	if !ok {
		return 0, fmt.Errorf("unexpected result_sets format")
	}

	return len(resultSets), nil
}

// GetQueryResultSetPage returns a page of a query result set.
func (c *Client) GetQueryResultSetPage(ctx context.Context, queryID string, resultSetIndex int, offset, limit int, rawFormat bool, requestID string) (map[string]interface{}, error) {
	params := c.buildParams()
	if offset > 0 {
		params["offset"] = strconv.Itoa(offset)
	}
	if limit > 0 {
		params["limit"] = strconv.Itoa(limit)
	}

	headers := c.buildHeaders("", requestID)
	url := c.composeAPIURL(fmt.Sprintf("/api/fq/v1/queries/%s/results/%d", queryID, resultSetIndex))

	resp, err := c.doRequest(ctx, "GET", url, headers, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := c.validateHTTPError(resp, http.StatusOK); err != nil {
		return nil, err
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return result, nil
}

// GetQueryResultSet returns a query result set.
func (c *Client) GetQueryResultSet(ctx context.Context, queryID string, resultSetIndex int, rawFormat bool) (map[string]interface{}, error) {
	offset := 0
	limit := 1000
	var columns interface{}
	var rows []interface{}

	for {
		part, err := c.GetQueryResultSetPage(ctx, queryID, resultSetIndex, offset, limit, rawFormat, "")
		if err != nil {
			return nil, err
		}

		if columns == nil {
			columns = part["columns"]
		}

		r, ok := part["rows"].([]interface{})
		if !ok {
			return nil, fmt.Errorf("unexpected rows format")
		}

		rows = append(rows, r...)

		if len(r) != limit {
			break
		}

		offset += limit
	}

	result := map[string]interface{}{
		"rows":    rows,
		"columns": columns,
	}

	if rawFormat {
		return result, nil
	}

	return NewYQResults(result).Results(), nil
}

// GetQueryAllResultSets returns all result sets of a query.
func (c *Client) GetQueryAllResultSets(ctx context.Context, queryID string, resultSetCount int, rawFormat bool) (interface{}, error) {
	if resultSetCount == 1 {
		return c.GetQueryResultSet(ctx, queryID, 0, rawFormat)
	}

	var results []interface{}
	for i := 0; i < resultSetCount; i++ {
		r, err := c.GetQueryResultSet(ctx, queryID, i, rawFormat)
		if err != nil {
			return nil, err
		}
		results = append(results, r)
	}

	return results, nil
}

// GetOpenAPISpec returns the OpenAPI specification of the YQ HTTP API.
func (c *Client) GetOpenAPISpec(ctx context.Context) (string, error) {
	resp, err := c.doRequest(ctx, "GET", c.composeAPIURL("/resources/v1/openapi.yaml"), nil, nil)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if err := c.validateHTTPError(resp, http.StatusOK); err != nil {
		return "", err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(body), nil
}

// ComposeQueryWebLink returns a web link to a query in the YQ web interface.
func (c *Client) ComposeQueryWebLink(queryID string) string {
	return c.composeWebURL(fmt.Sprintf("/folders/%s/ide/queries/%s", c.config.Project, queryID))
}
