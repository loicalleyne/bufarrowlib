package pbpath

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// buildTestDescriptors constructs a file descriptor for a test schema:
//
//	message Test {
//	  message Nested { string stringfield = 1; }
//	  Nested nested = 1;
//	  repeated Test repeats = 2;
//	  map<string, Nested> strkeymap = 3;
//	}
func buildTestDescriptors(t *testing.T) (protoreflect.MessageDescriptor, protoreflect.MessageDescriptor) {
	t.Helper()

	stringType := descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
	messageType := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
	labelOptional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
	labelRepeated := descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()

	fdp := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("test.proto"),
		Package: proto.String("pbpath.testdata"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("Test"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:     proto.String("nested"),
						Number:   proto.Int32(1),
						Type:     messageType,
						TypeName: proto.String(".pbpath.testdata.Test.Nested"),
						Label:    labelOptional,
					},
					{
						Name:     proto.String("repeats"),
						Number:   proto.Int32(2),
						Type:     messageType,
						TypeName: proto.String(".pbpath.testdata.Test"),
						Label:    labelRepeated,
					},
					{
						Name:     proto.String("strkeymap"),
						Number:   proto.Int32(3),
						Type:     messageType,
						TypeName: proto.String(".pbpath.testdata.Test.StrkeymapEntry"),
						Label:    labelRepeated,
					},
				},
				NestedType: []*descriptorpb.DescriptorProto{
					{
						Name: proto.String("Nested"),
						Field: []*descriptorpb.FieldDescriptorProto{
							{
								Name:   proto.String("stringfield"),
								Number: proto.Int32(1),
								Type:   stringType,
								Label:  labelOptional,
							},
						},
					},
					{
						Name: proto.String("StrkeymapEntry"),
						Field: []*descriptorpb.FieldDescriptorProto{
							{
								Name:   proto.String("key"),
								Number: proto.Int32(1),
								Type:   stringType,
								Label:  labelOptional,
							},
							{
								Name:     proto.String("value"),
								Number:   proto.Int32(2),
								Type:     messageType,
								TypeName: proto.String(".pbpath.testdata.Test.Nested"),
								Label:    labelOptional,
							},
						},
						Options: &descriptorpb.MessageOptions{
							MapEntry: proto.Bool(true),
						},
					},
				},
			},
		},
	}

	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		t.Fatalf("protodesc.NewFile: %v", err)
	}
	testMD := fd.Messages().ByName("Test")
	nestedMD := testMD.Messages().ByName("Nested")
	return testMD, nestedMD
}

// newNested creates a Nested message with the given stringfield value.
func newNested(nestedMD protoreflect.MessageDescriptor, val string) *dynamicpb.Message {
	m := dynamicpb.NewMessage(nestedMD)
	m.Set(nestedMD.Fields().ByName("stringfield"), protoreflect.ValueOfString(val))
	return m
}

// newTestWithNested creates a Test message with a nested field set.
func newTestWithNested(testMD, nestedMD protoreflect.MessageDescriptor, val string) *dynamicpb.Message {
	m := dynamicpb.NewMessage(testMD)
	nested := newNested(nestedMD, val)
	m.Set(testMD.Fields().ByName("nested"), protoreflect.ValueOfMessage(nested))
	return m
}

func TestPathValues(t *testing.T) {
	testMD, nestedMD := buildTestDescriptors(t)

	nestedFD := testMD.Fields().ByTextName("nested")
	repeatsFD := testMD.Fields().ByTextName("repeats")
	strkeymapFD := testMD.Fields().ByTextName("strkeymap")
	stringfieldFD := nestedMD.Fields().ByTextName("stringfield")

	// simple: Test with nested.stringfield = "stringfield"
	simple := newTestWithNested(testMD, nestedMD, "stringfield")
	simplerefl := protoreflect.ValueOf(simple)

	// rep: Test with repeats = [simple]
	rep := dynamicpb.NewMessage(testMD)
	repList := rep.Mutable(repeatsFD).List()
	repList.Append(protoreflect.ValueOfMessage(simple))

	// m: Test with strkeymap = {"k": Nested{stringfield: "stringfield"}}
	nested := newNested(nestedMD, "stringfield")
	m := dynamicpb.NewMessage(testMD)
	mapField := m.Mutable(strkeymapFD).Map()
	mapField.Set(protoreflect.ValueOfString("k").MapKey(), protoreflect.ValueOfMessage(nested))

	tcs := []struct {
		name      string
		msg       *dynamicpb.Message
		path      Path
		opts      []PathOption
		want      []Values // expected results (single-element for non-fan-out)
		wantErr   string
		wantPanic string
	}{
		{
			name: "root is self",
			msg:  simple,
			path: Path{Root(testMD)},
			want: []Values{{
				Path:   Path{Root(testMD)},
				Values: []protoreflect.Value{simplerefl},
			}},
		},
		{
			name: "field access",
			msg:  simple,
			path: Path{
				Root(testMD),
				FieldAccess(nestedFD),
			},
			want: []Values{{
				Path: Path{
					Root(testMD),
					FieldAccess(nestedFD),
				},
				Values: []protoreflect.Value{
					simplerefl,
					protoreflect.ValueOf(simple.Get(nestedFD).Message()),
				},
			}},
		},
		{
			name: "index access",
			msg:  rep,
			path: Path{
				Root(testMD),
				FieldAccess(repeatsFD),
				ListIndex(0),
				FieldAccess(nestedFD),
				FieldAccess(stringfieldFD),
			},
			want: []Values{{
				Path: Path{
					Root(testMD),
					FieldAccess(repeatsFD),
					ListIndex(0),
					FieldAccess(nestedFD),
					FieldAccess(stringfieldFD),
				},
				Values: []protoreflect.Value{
					protoreflect.ValueOf(rep.ProtoReflect()),
					rep.ProtoReflect().Get(repeatsFD),
					simplerefl,
					protoreflect.ValueOf(simple.Get(nestedFD).Message()),
					protoreflect.ValueOf("stringfield"),
				},
			}},
		},
		{
			name: "map access",
			msg:  m,
			path: Path{
				Root(testMD),
				FieldAccess(strkeymapFD),
				MapIndex(protoreflect.ValueOf("k").MapKey()),
				FieldAccess(stringfieldFD),
			},
			want: []Values{{
				Path: Path{
					Root(testMD),
					FieldAccess(strkeymapFD),
					MapIndex(protoreflect.ValueOf("k").MapKey()),
					FieldAccess(stringfieldFD),
				},
				Values: []protoreflect.Value{
					protoreflect.ValueOf(m.ProtoReflect()),
					m.ProtoReflect().Get(strkeymapFD),
					protoreflect.ValueOf(nested.ProtoReflect()),
					protoreflect.ValueOf("stringfield"),
				},
			}},
		},
		{
			name:    "repeated root",
			msg:     m,
			path:    Path{Root(testMD), Root(testMD)},
			wantErr: "root step at index 1",
		},
		{
			name: "index strmap with wrong key type",
			msg:  m,
			path: Path{
				Root(testMD),
				FieldAccess(strkeymapFD),
				MapIndex(protoreflect.ValueOf(int32(321)).MapKey()),
			},
			// dynamicpb does not panic on mismatched map key types; it returns
			// an invalid value which PathValues reports as a missing key error.
			wantErr: "missing key",
		},
		{
			name: "index missing key",
			msg:  m,
			path: Path{
				Root(testMD),
				FieldAccess(strkeymapFD),
				MapIndex(protoreflect.ValueOf("where").MapKey()),
			},
			wantErr: "missing key where",
		},
		{
			name: "index out of range non-strict",
			msg:  rep,
			path: Path{
				Root(testMD),
				FieldAccess(repeatsFD),
				ListIndex(1),
			},
			// Non-strict: out-of-range produces nil results, not an error.
		},
		{
			name: "index out of range strict",
			msg:  rep,
			path: Path{
				Root(testMD),
				FieldAccess(repeatsFD),
				ListIndex(1),
			},
			opts:    []PathOption{Strict()},
			wantErr: "out of range",
		},
		// ---- negative index ----
		{
			name: "negative index -1",
			msg:  rep,
			path: Path{
				Root(testMD),
				FieldAccess(repeatsFD),
				ListIndex(-1),
				FieldAccess(nestedFD),
				FieldAccess(stringfieldFD),
			},
			want: []Values{{
				Path: Path{
					Root(testMD),
					FieldAccess(repeatsFD),
					ListIndex(0), // resolved: -1 on len=1 → 0
					FieldAccess(nestedFD),
					FieldAccess(stringfieldFD),
				},
				Values: []protoreflect.Value{
					protoreflect.ValueOf(rep.ProtoReflect()),
					rep.ProtoReflect().Get(repeatsFD),
					simplerefl,
					protoreflect.ValueOf(simple.Get(nestedFD).Message()),
					protoreflect.ValueOf("stringfield"),
				},
			}},
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if tc.wantPanic != "" {
					if r == nil {
						t.Fatalf("expected panic did not happen: %q", tc.wantPanic)
					}
					msg := ""
					switch v := r.(type) {
					case error:
						msg = v.Error()
					case string:
						msg = v
					default:
						t.Fatalf("panic value has unexpected type %T: %v", r, r)
					}
					if !strings.Contains(msg, tc.wantPanic) {
						t.Fatalf("panic value unexpected: %v. Want %q", r, tc.wantPanic)
					}
				} else if r != nil {
					t.Fatalf("unexpected panic: %v", r)
				}
			}()
			got, err := PathValues(tc.path, tc.msg, tc.opts...)
			if !matchErr(err, tc.wantErr) {
				t.Fatalf("PathValues(%v, %v) = _, %v errored unexpectedly. Want %q", tc.path, tc.msg, err, tc.wantErr)
			}
			if tc.wantErr != "" {
				return
			}
			if tc.want == nil {
				// Expect nil/empty results (e.g. out-of-range non-strict).
				if len(got) != 0 {
					t.Fatalf("PathValues: expected no results, got %d", len(got))
				}
				return
			}
			type cmpValues struct {
				Pathstr string
				Value   []protoreflect.Value
			}
			if diff := cmp.Diff(tc.want, got, cmp.Transformer("Values",
				func(p Values) cmpValues {
					return cmpValues{Pathstr: p.Path.String(), Value: p.Values}
				})); diff != "" {
				t.Errorf("PathValues(%v, %v) returned diff (-want +got):\n%s", tc.path, tc.msg, diff)
			}
		})
	}
}

func TestPathValues_Wildcard(t *testing.T) {
	testMD, nestedMD := buildTestDescriptors(t)

	nestedFD := testMD.Fields().ByTextName("nested")
	repeatsFD := testMD.Fields().ByTextName("repeats")
	stringfieldFD := nestedMD.Fields().ByTextName("stringfield")

	a := newTestWithNested(testMD, nestedMD, "alpha")
	b := newTestWithNested(testMD, nestedMD, "beta")
	c := newTestWithNested(testMD, nestedMD, "gamma")

	msg := dynamicpb.NewMessage(testMD)
	list := msg.Mutable(repeatsFD).List()
	list.Append(protoreflect.ValueOfMessage(a))
	list.Append(protoreflect.ValueOfMessage(b))
	list.Append(protoreflect.ValueOfMessage(c))

	p := Path{
		Root(testMD),
		FieldAccess(repeatsFD),
		ListWildcard(),
		FieldAccess(nestedFD),
		FieldAccess(stringfieldFD),
	}

	results, err := PathValues(p, msg)
	if err != nil {
		t.Fatalf("PathValues wildcard: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}
	wantStrings := []string{"alpha", "beta", "gamma"}
	for i, r := range results {
		last := r.Index(-1)
		got := last.Value.String()
		if got != wantStrings[i] {
			t.Errorf("result[%d] = %q, want %q", i, got, wantStrings[i])
		}
		// Check that the concrete index in the path matches.
		idxStep := r.Path[2] // step after FieldAccess(repeatsFD)
		if idxStep.Kind() != ListIndexStep {
			t.Errorf("result[%d] step[2] kind = %d, want ListIndexStep", i, idxStep.Kind())
		}
		if idxStep.ListIndex() != i {
			t.Errorf("result[%d] step[2] index = %d, want %d", i, idxStep.ListIndex(), i)
		}
	}
}

func TestPathValues_Range(t *testing.T) {
	testMD, nestedMD := buildTestDescriptors(t)

	nestedFD := testMD.Fields().ByTextName("nested")
	repeatsFD := testMD.Fields().ByTextName("repeats")
	stringfieldFD := nestedMD.Fields().ByTextName("stringfield")

	msgs := []string{"alpha", "beta", "gamma", "delta"}
	parent := dynamicpb.NewMessage(testMD)
	list := parent.Mutable(repeatsFD).List()
	for _, s := range msgs {
		list.Append(protoreflect.ValueOfMessage(newTestWithNested(testMD, nestedMD, s)))
	}

	tcs := []struct {
		name   string
		step   Step
		want   []string
		strict bool
	}{
		{
			name: "range 1:3",
			step: ListRange(1, 3),
			want: []string{"beta", "gamma"},
		},
		{
			name: "range 0: (open)",
			step: ListRangeFrom(0),
			want: []string{"alpha", "beta", "gamma", "delta"},
		},
		{
			name: "range 2: (open)",
			step: ListRangeFrom(2),
			want: []string{"gamma", "delta"},
		},
		{
			name: "range -2: (open negative)",
			step: ListRangeFrom(-2),
			want: []string{"gamma", "delta"},
		},
		{
			name: "range -3:-1",
			step: ListRange(-3, -1),
			want: []string{"beta", "gamma"},
		},
		// ---- stride / step ----
		{
			name: "every other [::2]",
			step: ListRangeStep3(0, 0, 2, true, true),
			want: []string{"alpha", "gamma"},
		},
		{
			name: "reverse all [::-1]",
			step: ListRangeStep3(0, 0, -1, true, true),
			want: []string{"delta", "gamma", "beta", "alpha"},
		},
		{
			name: "start with step [1::2]",
			step: ListRangeStep3(1, 0, 2, false, true),
			want: []string{"beta", "delta"},
		},
		{
			name: "full slice with step [0:4:2]",
			step: ListRangeStep3(0, 4, 2, false, false),
			want: []string{"alpha", "gamma"},
		},
		{
			name: "reverse range [3:0:-1]",
			step: ListRangeStep3(3, 0, -1, false, false),
			want: []string{"delta", "gamma", "beta"},
		},
		{
			name: "inverted range positive step [5:2] empty",
			step: ListRange(5, 2),
			want: []string{}, // 5 >= 2 with step=1 produces nothing
		},
		{
			name: "negative step from end [-1::-1]",
			step: ListRangeStep3(-1, 0, -1, false, true),
			want: []string{"delta", "gamma", "beta", "alpha"},
		},
		{
			name: "reverse middle [2:0:-1]",
			step: ListRangeStep3(2, 0, -1, false, false),
			want: []string{"gamma", "beta"},
		},
		{
			name: "step 3 [::3]",
			step: ListRangeStep3(0, 0, 3, true, true),
			want: []string{"alpha", "delta"},
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			p := Path{
				Root(testMD),
				FieldAccess(repeatsFD),
				tc.step,
				FieldAccess(nestedFD),
				FieldAccess(stringfieldFD),
			}
			var opts []PathOption
			if tc.strict {
				opts = append(opts, Strict())
			}
			results, err := PathValues(p, parent, opts...)
			if err != nil {
				t.Fatalf("PathValues range: %v", err)
			}
			if len(results) != len(tc.want) {
				t.Fatalf("expected %d results, got %d", len(tc.want), len(results))
			}
			for i, r := range results {
				last := r.Index(-1)
				got := last.Value.String()
				if got != tc.want[i] {
					t.Errorf("result[%d] = %q, want %q", i, got, tc.want[i])
				}
			}
		})
	}
}

func TestPathValues_ListIndices(t *testing.T) {
	testMD, nestedMD := buildTestDescriptors(t)

	repeatsFD := testMD.Fields().ByTextName("repeats")

	a := newTestWithNested(testMD, nestedMD, "alpha")
	b := newTestWithNested(testMD, nestedMD, "beta")

	msg := dynamicpb.NewMessage(testMD)
	list := msg.Mutable(repeatsFD).List()
	list.Append(protoreflect.ValueOfMessage(a))
	list.Append(protoreflect.ValueOfMessage(b))

	p := Path{
		Root(testMD),
		FieldAccess(repeatsFD),
		ListWildcard(),
	}

	results, err := PathValues(p, msg)
	if err != nil {
		t.Fatalf("PathValues: %v", err)
	}
	for i, r := range results {
		indices := r.ListIndices()
		if len(indices) != 1 || indices[0] != i {
			t.Errorf("result[%d].ListIndices() = %v, want [%d]", i, indices, i)
		}
	}
}
