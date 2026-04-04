// pbpq is a local web playground for testing pbpath Pipeline queries
// against protobuf messages. It compiles a .proto file at startup,
// generates randomized sample data via protorand, and evaluates
// jq-style pipeline expressions interactively in the browser.
//
// Usage:
//
//	pbpq --proto path/to/file.proto [--import-path dir ...] [flags]
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/loicalleyne/bufarrowlib"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func main() {
	host := flag.String("host", "localhost", "server bind address")
	port := flag.String("port", "4195", "server port")
	noOpen := flag.Bool("no-open", false, "suppress auto-opening browser")
	seed := flag.Int64("seed", 0, "protorand seed (0 = random)")
	corpusFile := flag.String("corpus", "", "path to length-prefixed binary corpus file")
	corpusMsg := flag.String("corpus-message", "", "message name for corpus deserialization (required with --corpus)")

	var protoFiles multiFlag
	flag.Var(&protoFiles, "proto", "path to .proto file (repeatable, at least one required)")

	var importPaths multiFlag
	flag.Var(&importPaths, "import-path", "proto import directory (repeatable)")

	flag.Parse()

	if len(protoFiles) == 0 {
		fmt.Fprintln(os.Stderr, "error: at least one --proto is required")
		flag.Usage()
		os.Exit(1)
	}

	if *corpusFile != "" && *corpusMsg == "" {
		fmt.Fprintln(os.Stderr, "error: --corpus-message is required when --corpus is provided")
		flag.Usage()
		os.Exit(1)
	}

	// Compile all .proto files and collect messages.
	messageDescriptors := make(map[string]protoreflect.MessageDescriptor)
	var messageNames []string
	for _, pf := range protoFiles {
		fd, err := bufarrowlib.CompileProtoToFileDescriptor(pf, []string(importPaths))
		if err != nil {
			log.Fatalf("compile proto %s: %v", pf, err)
		}
		msgs := fd.Messages()
		for i := 0; i < msgs.Len(); i++ {
			name := string(msgs.Get(i).Name())
			if _, exists := messageDescriptors[name]; !exists {
				messageDescriptors[name] = msgs.Get(i)
				messageNames = append(messageNames, name)
			}
		}
	}
	if len(messageNames) == 0 {
		log.Fatal("no top-level messages found in proto files")
	}
	log.Printf("Loaded %d messages from %d proto files", len(messageNames), len(protoFiles))

	// Build server state.
	state := &serverState{
		messages:     messageDescriptors,
		messageNames: messageNames,
		seed:         *seed,
	}

	// Load corpus if provided.
	if *corpusFile != "" {
		corpus, err := loadCorpus(*corpusFile)
		if err != nil {
			log.Fatalf("load corpus: %v", err)
		}
		md, ok := messageDescriptors[*corpusMsg]
		if !ok {
			log.Fatalf("corpus message %q not found in loaded protos", *corpusMsg)
		}
		state.corpus = corpus
		state.corpusMsgName = *corpusMsg
		state.corpusMD = md
	}

	// Set up HTTP server.
	mux := http.NewServeMux()
	state.registerHandlers(mux)

	bindAddr := *host + ":" + *port
	server := http.Server{
		Addr:    bindAddr,
		Handler: mux,
	}

	if !*noOpen {
		u, err := url.Parse("http://" + bindAddr)
		if err == nil {
			openBrowser(u.String())
		}
	}

	log.Printf("Serving at: http://%s\n", bindAddr)
	log.Println("WARNING: This server is for local use only. Do not expose to the internet.")

	// Graceful shutdown on SIGINT/SIGTERM.
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		<-sigChan
		_ = server.Shutdown(context.Background())
	}()

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("listen: %v", err)
	}
}

func openBrowser(url string) {
	switch runtime.GOOS {
	case "linux":
		_ = exec.Command("xdg-open", url).Start()
	case "windows":
		_ = exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
	case "darwin":
		_ = exec.Command("open", url).Start()
	}
}

// multiFlag implements flag.Value for repeatable string flags.
type multiFlag []string

func (m *multiFlag) String() string { return fmt.Sprintf("%v", *m) }

func (m *multiFlag) Set(val string) error {
	*m = append(*m, val)
	return nil
}

// serverState holds immutable state created at startup.
type serverState struct {
	messages     map[string]protoreflect.MessageDescriptor
	messageNames []string
	seed         int64

	// Corpus mode (nil when not using corpus).
	corpus        [][]byte
	corpusMsgName string
	corpusMD      protoreflect.MessageDescriptor
}
