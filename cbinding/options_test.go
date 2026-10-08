//go:build cgo

package main

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory/mallocator"
	bufarrowlib "github.com/loicalleyne/bufarrowlib"
)

// newFromOpts builds a Transcoder for msg in cyclic.proto from an opts_json
// string, the way BufarrowNewFromFile does.
func newFromOpts(t *testing.T, msg, optsJSON string) (*bufarrowlib.Transcoder, error) {
	t.Helper()
	opts, err := parseOptsJSON(optsJSON)
	if err != nil {
		return nil, err
	}
	return bufarrowlib.NewFromFile("cyclic.proto", msg, []string{poolTestDir()}, mallocator.NewMallocator(), opts...)
}

func TestParseOptsJSONRejectsUnknownKeys(t *testing.T) {
	_, err := parseOptsJSON(`{"json_terminaton": ["Node"]}`)
	if err == nil || !strings.Contains(err.Error(), "json_terminaton") {
		t.Fatalf("parseOptsJSON(typo) error = %v, want unknown field error", err)
	}
}

func TestParseOptsJSONJSONTermination(t *testing.T) {
	if _, err := newFromOpts(t, "Node", ""); !errors.Is(err, bufarrowlib.ErrCyclicType) {
		t.Fatalf("without option error = %v, want ErrCyclicType", err)
	}
	tc, err := newFromOpts(t, "Node", `{"json_termination": ["Node"]}`)
	if err != nil {
		t.Fatalf("json_termination error = %v", err)
	}
	defer tc.Release()
	f, _ := tc.Schema().FieldsByName("children")
	if lt, ok := f[0].Type.(*arrow.ListType); !ok || lt.Elem().ID() != arrow.STRING {
		t.Errorf("children = %v, want list<string>", f[0].Type)
	}
}

func TestParseOptsJSONJSONTerminationAuto(t *testing.T) {
	tc, err := newFromOpts(t, "Tree", `{"json_termination_auto": true}`)
	if err != nil {
		t.Fatalf("json_termination_auto error = %v", err)
	}
	tc.Release()
}

func TestParseOptsJSONBoolOptions(t *testing.T) {
	tc, err := newFromOpts(t, "Flat", `{"well_known_types": true, "prune_empty_messages": true}`)
	if err != nil {
		t.Fatalf("bool options error = %v", err)
	}
	tc.Release()
}

func TestCyclicTypesJSON(t *testing.T) {
	got, err := cyclicTypes("cyclic.proto", "Tree", []string{poolTestDir()})
	if err != nil {
		t.Fatalf("cyclicTypes error = %v", err)
	}
	var names []string
	if err := json.Unmarshal([]byte(got), &names); err != nil {
		t.Fatalf("result %q is not a JSON array: %v", got, err)
	}
	if strings.Join(names, ",") != "Node,Ping" {
		t.Errorf("cyclicTypes = %v, want [Node Ping]", names)
	}
	if got, _ := cyclicTypes("cyclic.proto", "Flat", []string{poolTestDir()}); got != "[]" {
		t.Errorf("cyclicTypes(Flat) = %s, want []", got)
	}
	if _, err := cyclicTypes("cyclic.proto", "Missing", []string{poolTestDir()}); err == nil {
		t.Error("cyclicTypes(Missing) error = nil")
	}
}

func TestGlobalErrorInfo(t *testing.T) {
	_, err := newFromOpts(t, "Node", "")
	setGlobalErr(err)
	if msg := getGlobalErr(); !strings.Contains(msg, "cyclic message type") {
		t.Errorf("message = %q", msg)
	}
	var info errorInfo
	if err := json.Unmarshal([]byte(getGlobalErrInfo()), &info); err != nil {
		t.Fatalf("info is not JSON: %v", err)
	}
	if info.Kind != "cyclic_type" || info.Type != "Node" || info.Path != "children" {
		t.Errorf("info = %+v, want {cyclic_type Node children}", info)
	}
	if getGlobalErrInfo() != "" {
		t.Error("info not cleared after read")
	}
	setGlobalErr(errors.New("plain"))
	getGlobalErr()
	if s := getGlobalErrInfo(); s != "" {
		t.Errorf("plain error info = %q, want empty", s)
	}
}
