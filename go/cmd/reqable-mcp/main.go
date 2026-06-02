package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ssddi456/reqable-mcp/internal/config"
	"github.com/ssddi456/reqable-mcp/internal/ingest"
	"github.com/ssddi456/reqable-mcp/internal/mcpserver"
	"github.com/ssddi456/reqable-mcp/internal/storage"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	store, err := storage.New(cfg.DBPath, cfg.MaxBodySize, cfg.SummaryBodyPreviewLength, cfg.KeyBodyPreviewLength, cfg.RetentionDays)
	if err != nil {
		log.Fatalf("init storage: %v", err)
	}
	defer store.Close()

	ingestManager := ingest.NewManager(cfg, store)
	go func() {
		if err := ingestManager.Start(); err != nil {
			log.Printf("ingest server stopped with error: %v", err)
		}
	}()

	sse := mcpserver.New(cfg, store, ingestManager)

	errCh := make(chan error, 1)
	go func() {
		errCh <- sse.Start(cfg.MCPListenAddr())
	}()

	log.Printf("Reqable ingest listening on %s", cfg.IngestURL())
	log.Printf("Reqable MCP SSE listening on %s/sse", cfg.MCPBaseURL())

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		if err != nil {
			log.Fatalf("mcp sse server failed: %v", err)
		}
	case sig := <-sigCh:
		log.Printf("shutting down after %s", sig)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := sse.Shutdown(ctx); err != nil {
		log.Printf("mcp shutdown error: %v", err)
	}
	if err := ingestManager.Shutdown(ctx); err != nil {
		log.Printf("ingest shutdown error: %v", err)
	}
}
