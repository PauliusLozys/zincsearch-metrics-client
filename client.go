package zincmetric

import (
	"bytes"
	"fmt"
	"net/http"
	"net/url"
)

// Client provides io.Writer interface implementation
// to allow writing metring to ZincSearch service.
type Client struct {
	client     *http.Client
	user, pass string

	// ZincSearch endpoints (should be pre-built using buildEndpoints())
	healthURL         string // /healthx
	singleDocumentURL string // /api/{index}/_doc
}

// New creates a new client to export metrics to ZincSearch service.
func New(
	host string,
	user string,
	pass string,
	index string,
	ops ...OptionFunc,
) (*Client, error) {

	exporter := &Client{
		client: &http.Client{},
		user:   user,
		pass:   pass,
	}

	for _, op := range ops {
		op(exporter)
	}

	if err := exporter.buildEndpoints(host, index); err != nil {
		return nil, err
	}

	if err := exporter.ping(); err != nil {
		return nil, err
	}

	return exporter, nil
}

// Write writes data to ZincSearch service.
// Data is expected to be in JSON format.
func (l *Client) Write(data []byte) (int, error) {
	if err := l.createDocument(data); err != nil {
		return 0, err
	}
	return len(data), nil
}

// buildEndpoints pre-builds endpoints to be used for communicating
// with ZincSearch service.
func (l *Client) buildEndpoints(host, index string) error {
	var err error
	l.singleDocumentURL, err = url.JoinPath(host, "api", index, "_doc")
	if err != nil {
		return err
	}

	l.healthURL, err = url.JoinPath(host, "/healthz")
	if err != nil {
		return err
	}

	return nil
}

// createDocument posts a new document to ZincSearch service.
func (l *Client) createDocument(data []byte) error {
	req, err := http.NewRequest(http.MethodPost, l.singleDocumentURL, bytes.NewReader(data))
	if err != nil {
		return err
	}

	req.SetBasicAuth(l.user, l.pass)
	req.Header.Set("Content-Type", "application/json")

	resp, err := l.client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("not 200 response code: %d", resp.StatusCode)
	}

	return nil
}

// ping does a health check ping to the ZincSearch /healthz endpoint.
// Non 200 status code is treated as error.
func (l *Client) ping() error {
	req, err := http.NewRequest(http.MethodGet, l.healthURL, nil)
	if err != nil {
		return err
	}
	resp, err := l.client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("not 200 response code: %d", resp.StatusCode)
	}

	return nil
}
