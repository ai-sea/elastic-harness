package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

func main() {
	address := flag.String("listen", "127.0.0.1:8080", "HTTP 监听地址")
	dataDirectory := flag.String("data", "local-data", "standalone 数据目录")
	flag.Parse()
	if err := os.MkdirAll(*dataDirectory, 0o700); err != nil {
		log.Fatal(err)
	}
	toolEndpoint := "http://" + *address + "/internal/tools/echo"
	options := applicationOptions{
		databasePath: filepath.Join(*dataDirectory, "harness.sqlite"),
		artifactPath: filepath.Join(*dataDirectory, "artifacts"),
		toolEndpoint: toolEndpoint, logger: log.Default(),
	}
	if err := validateOptions(options); err != nil {
		log.Fatal(err)
	}
	app, err := newApplication(options)
	if err != nil {
		log.Fatal(err)
	}
	defer app.Close()
	server := &http.Server{Addr: *address, Handler: app.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Printf("ElasticHarness standalone 正在监听 %s", *address)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()
	shutdown, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-shutdown.Done()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("HTTP 服务关闭失败：%v", err)
	}
}
