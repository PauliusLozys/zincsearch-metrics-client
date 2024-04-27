package zincmetric

import "net/http"

type OptionFunc func(c *Client)

func WithHttpClient(h *http.Client) OptionFunc {
	return func(c *Client) {
		c.client = h
	}
}
