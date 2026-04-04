package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"

	_ "embed"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/loicalleyne/bufarrowlib"
	"github.com/loicalleyne/bufarrowlib/proto/pbpath"
	"github.com/sryoya/protorand"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"gopkg.in/yaml.v3"
)

//go:embed resources/editor_page.html
var editorPageHTML string

// registerHandlers wires all HTTP routes onto the given mux.
func (s *serverState) registerHandlers(mux *http.ServeMux) {
	indexTmpl := template.Must(template.New("index").Parse(editorPageHTML))

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		err := indexTmpl.Execute(w, struct {
			MessageNames    []string
			DefaultMessage  string
			HasCorpus       bool
			CorpusTotal     int
			InitialPipeline string
		}{
			MessageNames:    s.messageNames,
			DefaultMessage:  s.messageNames[0],
			HasCorpus:       s.corpus != nil,
			CorpusTotal:     len(s.corpus),
			InitialPipeline: ".",
		})
		if err != nil {
			http.Error(w, "template error", http.StatusInternalServerError)
		}
	})

	mux.HandleFunc("/api/messages", s.handleMessages)
	mux.HandleFunc("/api/generate", s.handleGenerate)
	mux.HandleFunc("/api/execute", s.handleExecute)
	mux.HandleFunc("/api/corpus/", s.handleCorpus)
	mux.HandleFunc("/api/denorm", s.handleDenorm)
	mux.HandleFunc("/api/denorm/skeleton", s.handleSkeleton)
}

func (s *serverState) handleMessages(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.messageNames)
}

func (s *serverState) handleGenerate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Message string `json:"message"`
		Seed    int64  `json:"seed"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, map[string]string{"error": err.Error()})
		return
	}

	md, ok := s.messages[req.Message]
	if !ok {
		writeJSON(w, map[string]string{"error": fmt.Sprintf("message %q not found", req.Message)})
		return
	}

	pr := protorand.New()
	seed := req.Seed
	if seed == 0 {
		seed = s.seed
	}
	if seed != 0 {
		pr.Seed(seed)
	} else {
		pr.Seed(rand.Int64())
	}
	pr.MaxCollectionElements = 3
	pr.MaxDepth = 5

	msg, err := pr.NewDynamicProtoRand(md)
	if err != nil {
		writeJSON(w, map[string]string{"error": fmt.Sprintf("generate: %v", err)})
		return
	}

	jsonBytes, err := protojson.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(msg)
	if err != nil {
		writeJSON(w, map[string]string{"error": fmt.Sprintf("json marshal: %v", err)})
		return
	}

	textBytes, err := prototext.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(msg)
	if err != nil {
		writeJSON(w, map[string]string{"error": fmt.Sprintf("text marshal: %v", err)})
		return
	}

	writeJSON(w, map[string]string{
		"json":   string(jsonBytes),
		"textpb": string(textBytes),
		"error":  "",
	})
}

func (s *serverState) handleExecute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Message      string `json:"message"`
		Input        string `json:"input"`
		Format       string `json:"format"`        // "json" or "textpb" — input format
		OutputFormat string `json:"output_format"` // "json" or "textpb" — output format
		Pipeline     string `json:"pipeline"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, map[string]string{"parse_error": err.Error()})
		return
	}

	md, ok := s.messages[req.Message]
	if !ok {
		writeJSON(w, map[string]string{"exec_error": fmt.Sprintf("message %q not found", req.Message)})
		return
	}

	// Parse pipeline.
	pipe, err := pbpath.ParsePipeline(md, req.Pipeline)
	if err != nil {
		writeJSON(w, map[string]string{
			"parse_error": err.Error(),
			"exec_error":  "",
			"result":      "",
		})
		return
	}

	// Deserialize input into a dynamic message.
	msg := dynamicpb.NewMessage(md)
	switch req.Format {
	case "textpb":
		err = prototext.Unmarshal([]byte(req.Input), msg)
	default: // "json" or empty
		err = protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal([]byte(req.Input), msg)
	}
	if err != nil {
		writeJSON(w, map[string]string{
			"parse_error": "",
			"exec_error":  fmt.Sprintf("unmarshal input: %v", err),
			"result":      "",
		})
		return
	}

	// Execute pipeline.
	results, err := pipe.ExecMessage(msg.ProtoReflect())
	if err != nil {
		writeJSON(w, map[string]string{
			"parse_error": "",
			"exec_error":  err.Error(),
			"result":      "",
		})
		return
	}

	// Format results.
	outFmt := req.OutputFormat
	if outFmt == "" {
		outFmt = "json"
	}
	var sb strings.Builder
	for i, v := range results {
		if i > 0 {
			sb.WriteByte('\n')
		}
		sb.WriteString(formatValue(v, outFmt))
	}

	writeJSON(w, map[string]string{
		"parse_error": "",
		"exec_error":  "",
		"result":      sb.String(),
	})
}

func (s *serverState) handleCorpus(w http.ResponseWriter, r *http.Request) {
	if s.corpus == nil {
		http.Error(w, "no corpus loaded", http.StatusNotFound)
		return
	}

	// Parse index from URL path: /api/corpus/42
	idxStr := strings.TrimPrefix(r.URL.Path, "/api/corpus/")
	idx, err := strconv.Atoi(idxStr)
	if err != nil || idx < 0 || idx >= len(s.corpus) {
		writeJSON(w, map[string]any{
			"error": fmt.Sprintf("index out of range [0, %d)", len(s.corpus)),
		})
		return
	}

	msg := dynamicpb.NewMessage(s.corpusMD)
	if err := proto.Unmarshal(s.corpus[idx], msg); err != nil {
		writeJSON(w, map[string]any{"error": fmt.Sprintf("unmarshal: %v", err)})
		return
	}

	jsonBytes, err := protojson.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(msg)
	if err != nil {
		writeJSON(w, map[string]any{"error": fmt.Sprintf("json marshal: %v", err)})
		return
	}

	textBytes, err := prototext.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(msg)
	if err != nil {
		writeJSON(w, map[string]any{"error": fmt.Sprintf("text marshal: %v", err)})
		return
	}

	writeJSON(w, map[string]any{
		"json":   string(jsonBytes),
		"textpb": string(textBytes),
		"index":  idx,
		"total":  len(s.corpus),
		"error":  "",
	})
}

// loadCorpus reads a length-prefixed binary corpus file into memory.
// Format: [4-byte LE uint32 length][N bytes proto.Marshal output] repeated.
func loadCorpus(path string) ([][]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var corpus [][]byte
	var lenBuf [4]byte
	for {
		if _, err := io.ReadFull(f, lenBuf[:]); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("read length at message %d: %w", len(corpus), err)
		}
		n := binary.LittleEndian.Uint32(lenBuf[:])
		data := make([]byte, n)
		if _, err := io.ReadFull(f, data); err != nil {
			return nil, fmt.Errorf("read body at message %d: %w", len(corpus), err)
		}
		corpus = append(corpus, data)
	}
	return corpus, nil
}

// formatValue renders a pbpath.Value as a human-readable string.
// For MessageKind values it serializes using protojson or prototext
// instead of the default "<message>" placeholder.
func formatValue(v pbpath.Value, format string) string {
	switch v.Kind() {
	case pbpath.MessageKind:
		m := v.Message()
		if m == nil {
			return "null"
		}
		var b []byte
		var err error
		if format == "textpb" {
			b, err = prototext.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(m.Interface())
		} else {
			b, err = protojson.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(m.Interface())
		}
		if err != nil {
			return fmt.Sprintf("<message: marshal error: %v>", err)
		}
		return string(b)
	case pbpath.ListKind:
		items := v.List()
		if len(items) == 0 {
			return "[]"
		}
		var sb strings.Builder
		sb.WriteString("[\n")
		for i, elem := range items {
			if i > 0 {
				sb.WriteString(",\n")
			}
			sb.WriteString("  ")
			sb.WriteString(formatValue(elem, format))
		}
		sb.WriteString("\n]")
		return sb.String()
	case pbpath.ObjectKind:
		entries := v.Entries()
		if len(entries) == 0 {
			return "{}"
		}
		var sb strings.Builder
		sb.WriteString("{\n")
		for i, e := range entries {
			if i > 0 {
				sb.WriteString(",\n")
			}
			fmt.Fprintf(&sb, "  %q: %s", e.Key, formatValue(e.Value, format))
		}
		sb.WriteString("\n}")
		return sb.String()
	default:
		return v.String()
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

// handleDenorm processes a denorm config against a protobuf message and returns
// the resulting Arrow record batch as a JSON table.
func (s *serverState) handleDenorm(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Message string `json:"message"`
		Input   string `json:"input"`
		Format  string `json:"format"`
		Config  string `json:"config"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, map[string]any{"config_error": err.Error()})
		return
	}

	md, ok := s.messages[req.Message]
	if !ok {
		writeJSON(w, map[string]any{"config_error": fmt.Sprintf("message %q not found", req.Message)})
		return
	}

	// Parse YAML columns config.
	var cfg struct {
		Columns []bufarrowlib.ColumnDef `yaml:"columns"`
	}
	if err := yaml.Unmarshal([]byte(req.Config), &cfg); err != nil {
		writeJSON(w, map[string]any{"config_error": fmt.Sprintf("YAML parse error: %v", err)})
		return
	}
	if len(cfg.Columns) == 0 {
		writeJSON(w, map[string]any{"config_error": "at least one column is required"})
		return
	}

	// Convert columns to plan specs.
	specs, err := bufarrowlib.ColumnsToPlanSpecs(cfg.Columns)
	if err != nil {
		writeJSON(w, map[string]any{"config_error": err.Error()})
		return
	}

	// Create Transcoder with denorm plan.
	tc, err := bufarrowlib.New(md, memory.DefaultAllocator, bufarrowlib.WithDenormalizerPlan(specs...))
	if err != nil {
		writeJSON(w, map[string]any{"config_error": fmt.Sprintf("create transcoder: %v", err)})
		return
	}

	// Deserialize input message.
	msg := dynamicpb.NewMessage(md)
	switch req.Format {
	case "textpb":
		err = prototext.Unmarshal([]byte(req.Input), msg)
	default:
		err = protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal([]byte(req.Input), msg)
	}
	if err != nil {
		writeJSON(w, map[string]any{
			"config_error": "",
			"exec_error":   fmt.Sprintf("unmarshal input: %v", err),
		})
		return
	}

	// Append message and produce record batch.
	if err := tc.AppendDenorm(msg); err != nil {
		writeJSON(w, map[string]any{
			"config_error": "",
			"exec_error":   fmt.Sprintf("append: %v", err),
		})
		return
	}

	rec := tc.NewDenormalizerRecordBatch()
	if rec == nil {
		writeJSON(w, map[string]any{
			"config_error": "",
			"exec_error":   "no denorm output (record batch is nil)",
		})
		return
	}
	defer rec.Release()

	columns, rows := recordBatchToTable(rec)
	writeJSON(w, map[string]any{
		"config_error": "",
		"exec_error":   "",
		"columns":      columns,
		"rows":         rows,
		"num_rows":     rec.NumRows(),
	})
}

// handleSkeleton generates a starter YAML denorm config for the selected message.
func (s *serverState) handleSkeleton(w http.ResponseWriter, r *http.Request) {
	msgName := r.URL.Query().Get("message")
	md, ok := s.messages[msgName]
	if !ok {
		writeJSON(w, map[string]any{"error": fmt.Sprintf("message %q not found", msgName)})
		return
	}

	var sb strings.Builder
	sb.WriteString("columns:\n")

	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		name := string(fd.Name())

		switch {
		case fd.IsList() && fd.Kind() == protoreflect.MessageKind:
			// Repeated message: use first scalar sub-field as example.
			subMD := fd.Message()
			subFields := subMD.Fields()
			for j := 0; j < subFields.Len(); j++ {
				sf := subFields.Get(j)
				if sf.Kind() != protoreflect.MessageKind {
					subName := string(sf.Name())
					sb.WriteString(fmt.Sprintf("  - name: %s_%s\n    path: %s[*].%s\n", name, subName, name, subName))
					break
				}
			}
		case fd.IsList():
			// Repeated scalar.
			sb.WriteString(fmt.Sprintf("  - name: %s\n    path: %s[*]\n", name, name))
		case fd.Kind() == protoreflect.MessageKind:
			// Singular message (commented out).
			sb.WriteString(fmt.Sprintf("  # - name: %s_field\n  #   path: %s.{field}\n", name, name))
		default:
			// Scalar field.
			sb.WriteString(fmt.Sprintf("  - name: %s\n    path: %s\n", name, name))
		}
	}

	writeJSON(w, map[string]any{
		"config": sb.String(),
		"error":  "",
	})
}

// recordBatchToTable converts an Arrow record batch to column names and string rows.
func recordBatchToTable(rec arrow.Record) (columns []string, rows [][]string) {
	ncols := int(rec.NumCols())
	nrows := int(rec.NumRows())
	columns = make([]string, ncols)
	for i := 0; i < ncols; i++ {
		columns[i] = rec.ColumnName(i)
	}
	rows = make([][]string, nrows)
	for r := 0; r < nrows; r++ {
		row := make([]string, ncols)
		for c := 0; c < ncols; c++ {
			col := rec.Column(c)
			if col.IsNull(r) {
				row[c] = "null"
			} else {
				row[c] = col.ValueStr(r)
			}
		}
		rows[r] = row
	}
	return columns, rows
}
