package splunk

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/net/context/ctxhttp"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
)

// Client is the Splunk REST API client
type Client struct {
	httpClient *http.Client
	username   string
	password   string
	authURL    string
	searchURL  string
	sessionKey string
}

// NewClient returns a new Splunk REST API client.
func NewClient(username string, password string, baseURL string) *Client {
	return &Client{
		httpClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		},
		username:  username,
		password:  password,
		authURL:   baseURL + "/services/auth/login",
		searchURL: baseURL + "/services/search/jobs/export",
	}
}

// Login creates a new session.
func (c *Client) Login(ctx context.Context) error {
	var m map[string]string
	var ok bool
	data := make(url.Values)
	data.Add("username", c.username)
	data.Add("password", c.password)
	data.Add("output_mode", "json")

	resp, err := ctxhttp.PostForm(ctx, c.httpClient, c.authURL, data)
	if err != nil {
		return err
	}

	body, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.New(string(body))
	}

	if err := json.Unmarshal(body, &m); err != nil {
		return err
	}

	if c.sessionKey, ok = m["sessionKey"]; !ok || c.sessionKey == "" {
		return fmt.Errorf("login failed: %s", string(body))
	}

	return nil
}

// Search streams search results to io.Writer as they become available.
func (c *Client) Search(ctx context.Context, q string, from string, w io.Writer) error {
	data := make(url.Values)
	data.Add("search", fmt.Sprintf("search %s", q))
	data.Add("earliest_time", from)
	data.Add("output_mode", "json")

	req, err := http.NewRequest(http.MethodPost, c.searchURL, bytes.NewBufferString(data.Encode()))
	if err != nil {
		return err
	}

	//if c.sessionKey != "" {
	//	req.Header.Add("Authorization", fmt.Sprintf("Splunk %s", c.sessionKey))
	//} else {
	req.SetBasicAuth(c.username, c.password)
	//}

	resp, err := ctxhttp.Do(ctx, c.httpClient, req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.New(resp.Status)
	}

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()

		default:
			if _, err := w.Write(scanner.Bytes()); err != nil {
				return err
			}
		}
	}

	return nil
}
