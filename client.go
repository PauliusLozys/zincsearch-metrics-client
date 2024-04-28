package zincmetric

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// Client provides io.Writer interface implementation
// to allow writing metring to ZincSearch service.
type Client struct {
	user, pass string
	index      string

	// Option configurable
	client        *http.Client
	flushInterval time.Duration

	dataCh  chan []byte
	closeCh chan struct{}

	// ZincSearch endpoints (should be pre-built using buildEndpoints())
	healthURL         string // /healthx
	singleDocumentURL string // /api/{index}/_doc
	bulkDocumentsURL  string // /api/_bulkv2
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
		user:          user,
		pass:          pass,
		index:         index,
		client:        &http.Client{},
		flushInterval: time.Second,
		dataCh:        make(chan []byte),
		closeCh:       make(chan struct{}),
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

	go exporter.run()

	return exporter, nil
}

// Write writes data to ZincSearch service.
// Data is expected to be in JSON format.
func (c *Client) Write(data []byte) (int, error) {
	select {
	case <-c.closeCh:
		return 0, errors.New("client closed")
	case c.dataCh <- bytes.Clone(data):
		return len(data), nil
	}
}

// Close closes the metrics client and flushes all
// remaining metrics to ZincSearch service.
func (c *Client) Close() error {
	close(c.closeCh)
	return nil
}

// buildEndpoints pre-builds endpoints to be used for communicating
// with ZincSearch service.
func (c *Client) buildEndpoints(host, index string) error {
	var err error
	c.singleDocumentURL, err = url.JoinPath(host, "api", index, "_doc")
	if err != nil {
		return err
	}

	c.bulkDocumentsURL, err = url.JoinPath(host, "api", "_bulkv2")
	if err != nil {
		return err
	}

	c.healthURL, err = url.JoinPath(host, "/healthz")
	if err != nil {
		return err
	}

	return nil
}

// createDocument posts a new document to ZincSearch service.
func (c *Client) createDocument(data []byte) error {
	req, err := http.NewRequest(http.MethodPost, c.singleDocumentURL, bytes.NewReader(data))
	if err != nil {
		return err
	}

	req.SetBasicAuth(c.user, c.pass)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("not 200 response code: %d", resp.StatusCode)
	}

	return nil
}

// createBulkDocuments posts a bulk of new documents to ZincSearch service.
func (c *Client) createBulkDocuments(data [][]byte) error {
	// Construct request body, this should be faster and simpler than unmarshaling each data peace individually.
	// Format:
	// {
	//	"index": "string",
	//	"records": [
	//		{
	//	  	"additionalProp1": {}
	//		}
	//	]
	// }
	buff := new(bytes.Buffer)
	_, err := buff.WriteString(fmt.Sprintf(`{"index":"%s","records":[`, c.index))
	if err != nil {
		return err
	}

	_, err = buff.Write(append(bytes.Join(data, []byte(`,`)), []byte(`]}`)...))
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPost, c.bulkDocumentsURL, buff)
	if err != nil {
		return err
	}

	req.SetBasicAuth(c.user, c.pass)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
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
func (c *Client) ping() error {
	req, err := http.NewRequest(http.MethodGet, c.healthURL, nil)
	if err != nil {
		return err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("not 200 response code: %d", resp.StatusCode)
	}

	return nil
}

// run runs pusher tread, that gathers and pushes data to ZincSearch service.
func (c *Client) run() {
	buff := make([][]byte, 0)

	tick := time.NewTicker(c.flushInterval)
	defer tick.Stop()

	defer func() {
		// Flush remaining buffer.
		c.createBulkDocuments(buff)
	}()

	for {
		select {
		case <-c.closeCh:
			return
		case b := <-c.dataCh:
			buff = append(buff, b)
		case <-tick.C:
			if err := c.flushBuffer(buff); err != nil {
				// TODO: would be nice to log this, should potentially introduce Logger interface.
				break // Don't clear the buffer in case of error.
			}
			buff = nil
		}
	}
}

// flushBuffer pushes data in buffer to ZincSearch service.
func (c *Client) flushBuffer(buff [][]byte) error {
	if len(buff) == 0 {
		return nil
	}

	if len(buff) == 1 {
		return c.createDocument(buff[0])
	}

	return c.createBulkDocuments(buff)
}
