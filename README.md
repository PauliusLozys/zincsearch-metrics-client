# ZincSearch metrics client

This metrics client provides `io.Writer` interface which writes documents to `ZincSearch` service.

### Go Get
```sh
go get github.com/PauliusLozys/zincsearch-metrics-client
```

### Example
```go
func main() {
	w, err := zincmetric.New("http://localhost:4080", "admin", "admin", "test-service")
	if err != nil {
		panic(err)
	}

	jsonLogger := slog.NewJSONHandler(w, nil)
	slog.SetDefault(slog.New(jsonLogger))

	slog.Info("test")
	slog.Info("test2")
	slog.Info("test3")
}
```

### Options
Custom HTTP client can be passed using `WithHttpClient`
