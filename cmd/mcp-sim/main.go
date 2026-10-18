// Command mcp-sim is the Model Context Protocol server for the Go Monte Carlo
// strategy engine. It serves JSON-RPC 2.0 over stdio (-stdio) or HTTP on
// :8081 (POST /mcp and GET /sse). With -brokers it also consumes the live
// telemetry stream so get_live_driver_window can answer from memory.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/yj3335/pitwall-ai/internal/kafka"
	"github.com/yj3335/pitwall-ai/internal/mcp"
	"github.com/yj3335/pitwall-ai/internal/pipeline"
	"github.com/yj3335/pitwall-ai/internal/stream"
)

func main() {
	httpAddr := flag.String("http", "127.0.0.1:8081", "HTTP listen address (loopback by default; 0.0.0.0:8081 in a container)")
	stdio := flag.Bool("stdio", false, "serve MCP over stdin/stdout instead of HTTP")
	brokers := flag.String("brokers", "", "Kafka brokers for live driver windows ('' disables)")
	quiet := flag.Bool("quiet", false, "do not log tool calls")
	flag.Parse()

	// SIGTERM too (make stop, containers): a clean exit leaves the consumer group
	// and flushes pending Parquet batches instead of losing them
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.SetOutput(os.Stderr) // stdout is the protocol channel in stdio mode

	var board *stream.Board
	if *brokers != "" {
		p := pipeline.New(pipeline.Config{})
		board = p.Board
		bl := strings.Split(*brokers, ",")
		cl, err := kafka.NewConsumer(bl, fmt.Sprintf("pitwall-mcp-live-%d", time.Now().Unix()),
			[]string{kafka.TopicTelemetry}, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
		if err != nil {
			log.Fatal(err)
		}
		go func() {
			if err := kafka.Run(ctx, cl, p.PartitionHandler); err != nil {
				log.Printf("live consumer: %v", err)
			}
		}()
		log.Printf("live driver windows from %s", *brokers)
	}

	srv := mcp.NewServer("pitwall-mcp-sim", "0.1.0")
	if *quiet {
		srv.Logf = nil
	}
	mcp.RegisterSimTools(srv, board)

	if *stdio {
		if err := srv.ServeStdio(ctx, os.Stdin, os.Stdout); err != nil && err != io.EOF {
			log.Fatal(err)
		}
		return
	}
	hs := &http.Server{Addr: *httpAddr, Handler: srv.HTTPHandler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		hs.Shutdown(sctx)
	}()
	log.Printf("MCP server on http://%s/mcp (SSE: /sse)", *httpAddr)
	if err := hs.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
