package zincmetric

import (
	"net/http"
	"time"
)

type OptionFunc func(c *Client)

func WithHttpClient(h *http.Client) OptionFunc {
	return func(c *Client) {
		c.client = h
	}
}

func WithFlushInterval(d time.Duration) OptionFunc {
	return func(c *Client) {
		c.flushInterval = d
	}
}
