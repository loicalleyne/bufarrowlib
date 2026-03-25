package pbpath

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

func matchErr(err error, want string) bool {
	return (err == nil && want == "") ||
		(want != "" && err != nil && strings.Contains(err.Error(), want))
}

// buildParseTestDescriptor constructs a message descriptor for:
//
//	package testprotopath;
//	message Test {
//	  message Nested {
//	    string stringfield = 1;
//	    int32  intfield     = 2;
//	    Test   nested       = 3;       // back-reference to Test
//	    map<string, Nested> strkeymap = 4;
//	  }
//	  Nested              nested        = 1;
//	  repeated Test       repeats       = 2;
//	  map<string, Nested> strkeymap     = 3;
//	  map<bool,   Nested> boolkeymap    = 4;
//	  repeated int32      int32repeats  = 5;
//	  map<uint64, Test>   uint64keymap  = 6;
//	  map<uint32, Nested> uint32keymap  = 7;
//	  map<int32,  Test>   int32keymap   = 8;
//	  map<int64,  Test>   int64keymap   = 9;
//	}
func buildParseTestDescriptor(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()

	stringType := descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
	messageType := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
	int32Type := descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum()
	boolType := descriptorpb.FieldDescriptorProto_TYPE_BOOL.Enum()
	uint64Type := descriptorpb.FieldDescriptorProto_TYPE_UINT64.Enum()
	uint32Type := descriptorpb.FieldDescriptorProto_TYPE_UINT32.Enum()
	int64Type := descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum()
	labelOptional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
	labelRepeated := descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()

	mapEntry := func(name string, keyType *descriptorpb.FieldDescriptorProto_Type, valueType *descriptorpb.FieldDescriptorProto_Type, valueTypeName string) *descriptorpb.DescriptorProto {
		entry := &descriptorpb.DescriptorProto{
			Name: proto.String(name),
			Field: []*descriptorpb.FieldDescriptorProto{
				{Name: proto.String("key"), Number: proto.Int32(1), Type: keyType, Label: labelOptional},
				{Name: proto.String("value"), Number: proto.Int32(2), Type: valueType, Label: labelOptional},
			},
			Options: &descriptorpb.MessageOptions{MapEntry: proto.Bool(true)},
		}
		if valueTypeName != "" {
			entry.Field[1].TypeName = proto.String(valueTypeName)
		}
		return entry
	}

	fdp := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("testprotopath.proto"),
		Package: proto.String("testprotopath"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("Test"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{Name: proto.String("nested"), Number: proto.Int32(1), Type: messageType, TypeName: proto.String(".testprotopath.Test.Nested"), Label: labelOptional},
					{Name: proto.String("repeats"), Number: proto.Int32(2), Type: messageType, TypeName: proto.String(".testprotopath.Test"), Label: labelRepeated},
					{Name: proto.String("strkeymap"), Number: proto.Int32(3), Type: messageType, TypeName: proto.String(".testprotopath.Test.StrkeymapEntry"), Label: labelRepeated},
					{Name: proto.String("boolkeymap"), Number: proto.Int32(4), Type: messageType, TypeName: proto.String(".testprotopath.Test.BoolkeymapEntry"), Label: labelRepeated},
					{Name: proto.String("int32repeats"), Number: proto.Int32(5), Type: int32Type, Label: labelRepeated},
					{Name: proto.String("uint64keymap"), Number: proto.Int32(6), Type: messageType, TypeName: proto.String(".testprotopath.Test.Uint64keymapEntry"), Label: labelRepeated},
					{Name: proto.String("uint32keymap"), Number: proto.Int32(7), Type: messageType, TypeName: proto.String(".testprotopath.Test.Uint32keymapEntry"), Label: labelRepeated},
					{Name: proto.String("int32keymap"), Number: proto.Int32(8), Type: messageType, TypeName: proto.String(".testprotopath.Test.Int32keymapEntry"), Label: labelRepeated},
					{Name: proto.String("int64keymap"), Number: proto.Int32(9), Type: messageType, TypeName: proto.String(".testprotopath.Test.Int64keymapEntry"), Label: labelRepeated},
				},
				NestedType: []*descriptorpb.DescriptorProto{
					{
						Name: proto.String("Nested"),
						Field: []*descriptorpb.FieldDescriptorProto{
							{Name: proto.String("stringfield"), Number: proto.Int32(1), Type: stringType, Label: labelOptional},
							{Name: proto.String("intfield"), Number: proto.Int32(2), Type: int32Type, Label: labelOptional},
							{Name: proto.String("nested"), Number: proto.Int32(3), Type: messageType, TypeName: proto.String(".testprotopath.Test"), Label: labelOptional},
							{Name: proto.String("strkeymap"), Number: proto.Int32(4), Type: messageType, TypeName: proto.String(".testprotopath.Test.Nested.StrkeymapEntry"), Label: labelRepeated},
						},
						NestedType: []*descriptorpb.DescriptorProto{
							mapEntry("StrkeymapEntry", stringType, messageType, ".testprotopath.Test.Nested"),
						},
					},
					mapEntry("StrkeymapEntry", stringType, messageType, ".testprotopath.Test.Nested"),
					mapEntry("BoolkeymapEntry", boolType, messageType, ".testprotopath.Test.Nested"),
					mapEntry("Uint64keymapEntry", uint64Type, messageType, ".testprotopath.Test"),
					mapEntry("Uint32keymapEntry", uint32Type, messageType, ".testprotopath.Test.Nested"),
					mapEntry("Int32keymapEntry", int32Type, messageType, ".testprotopath.Test"),
					mapEntry("Int64keymapEntry", int64Type, messageType, ".testprotopath.Test"),
				},
			},
		},
	}

	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		t.Fatalf("protodesc.NewFile: %v", err)
	}
	return fd.Messages().ByName("Test")
}

func TestParsePath(t *testing.T) {
	md := buildParseTestDescriptor(t)
	tcs := []struct {
		name    string
		md      protoreflect.MessageDescriptor
		path    string
		want    string
		wantErr string
	}{
		{
			name:    "empty",
			wantErr: "message descriptor must be non-nil",
		},
		{
			name: "root from empty",
			md:   md,
			want: "(testprotopath.Test)",
		},
		{
			name: "root from fullpath",
			md:   md,
			path: "(testprotopath.Test)",
			want: "(testprotopath.Test)",
		},
		{
			name: "implicit root",
			md:   md,
			path: "nested",
			want: "(testprotopath.Test).nested",
		},
		{
			name: "explicit root",
			md:   md,
			path: "(testprotopath.Test).nested",
			want: "(testprotopath.Test).nested",
		},
		{
			name:    "unknown field",
			md:      md,
			path:    "unknown",
			wantErr: "\"unknown\" not in message descriptor",
		},
		{
			name: "list index",
			md:   md,
			path: "repeats[5]",
			want: "(testprotopath.Test).repeats[5]",
		},
		{
			name: "map string index",
			md:   md,
			path: "strkeymap[\"key\"]",
			want: "(testprotopath.Test).strkeymap[\"key\"]",
		},
		{
			name: "map bool true index",
			md:   md,
			path: "boolkeymap[true]",
			want: "(testprotopath.Test).boolkeymap[true]",
		},
		{
			name: "map bool false index",
			md:   md,
			path: "boolkeymap[false]",
			want: "(testprotopath.Test).boolkeymap[false]",
		},
		{
			name: "map subindex",
			md:   md,
			path: "strkeymap[\"key\"].stringfield",
			want: "(testprotopath.Test).strkeymap[\"key\"].stringfield",
		},
		{
			name: "pod index",
			md:   md,
			path: "int32repeats[123]",
			want: "(testprotopath.Test).int32repeats[123]",
		},
		{
			name:    "double map index",
			md:      md,
			path:    "strkeymap[\"key\"][\"key2\"]",
			wantErr: "expected field descriptor to access with value \"key2\"",
		},
		{
			name: "big index",
			md:   md,
			path: "uint64keymap[0xffffffffffffffff]",
			want: "(testprotopath.Test).uint64keymap[18446744073709551615]",
		},
		{
			name:    "too big index",
			md:      md,
			path:    "uint64keymap[0xfffffffffffffffff]",
			wantErr: "cannot index map with key kind uint64 with key 0xfffffffffffffffff",
		},
		{
			name: "negative list index",
			md:   md,
			path: "repeats[-4]",
			want: "(testprotopath.Test).repeats[-4]",
		},
		{
			name:    "negative uint is bad",
			md:      md,
			path:    "uint32keymap[-4]",
			wantErr: "cannot index map with key kind uint32 with key -4",
		},
		{
			name: "uint32 index",
			md:   md,
			path: "uint32keymap[4]",
			want: "(testprotopath.Test).uint32keymap[4]",
		},
		{
			name: "negative int is fine",
			md:   md,
			path: "int32keymap[-4]",
			want: "(testprotopath.Test).int32keymap[-4]",
		},
		{
			name:    "really negative int is not fine",
			md:      md,
			path:    "int32keymap[-0xffffffff]",
			wantErr: "cannot index map with key kind int32 with key -0xffffffff",
		},
		{
			name: "really negative int is fine for 64",
			md:   md,
			path: "int64keymap[-0xffffffff]",
			want: "(testprotopath.Test).int64keymap[-4294967295]",
		},
		{
			name:    "reaaaaally negative int is bad for 64",
			md:      md,
			path:    "int64keymap[-0xffffffffffffffff]",
			wantErr: "cannot index map with key kind int64 with key -0xffffffffffffffff",
		},
		{
			name:    "string index for int map is bad",
			md:      md,
			path:    "int32keymap[\"foo\"]",
			wantErr: "cannot index map with key kind int32 with key \"foo\"",
		},
		{
			name:    "bool index for uint map is bad",
			md:      md,
			path:    "uint32keymap[true]",
			wantErr: "cannot index map with key kind uint32 with key true",
		},
		{
			name: "recursion! with octal literals!",
			md:   md,
			path: `int32keymap[-6].uint64keymap[040000000000].repeats[0].nested.nested.strkeymap["k"].intfield`,
			want: `(testprotopath.Test).int32keymap[-6].uint64keymap[4294967296].repeats[0].nested.nested.strkeymap["k"].intfield`,
		},
		{
			name:    "unexpected string index",
			md:      md,
			path:    `int32repeats["key"]`,
			wantErr: "non-integral type \"key\"",
		},
		{
			name:    "unexpected string field",
			md:      md,
			path:    `nested."stringfield"`,
			wantErr: "expect field name following '.'",
		},
		{
			name:    "unexpected string access",
			md:      md,
			path:    `nested"stringfield"`,
			wantErr: "expect one of '[', '.', or eof",
		},
		{
			name:    "root weirdness",
			md:      md,
			path:    "(a.1)",
			wantErr: "expect next name fragment of message descriptor's full name",
		},
		{
			name:    "no root name",
			md:      md,
			path:    "()",
			wantErr: "expect next name fragment of message descriptor's full name",
		},
		{
			name:    "root name dot",
			md:      md,
			path:    `(a"str")`,
			wantErr: "expect either '.' for next full name fragment or ')'",
		},
		{
			name:    "no index close",
			md:      md,
			path:    `int32repeats[32)`,
			wantErr: "expect ']'",
		},
		{
			name:    "no field",
			md:      md,
			path:    `nested.`,
			wantErr: "finished parsing in state that expects field name following '.'",
		},
		{
			name:    "can't index int32",
			md:      md,
			path:    `int32repeats[5].nested`,
			wantErr: "column 16: int32repeats[5].nested\n                           ^----|\nexpected message descriptor to access with field \"nested\"",
		},
		{
			name:    "can't access map",
			md:      md,
			path:    "strkeymap.nested",
			wantErr: "field \"nested\" not in message descriptor",
		},
		{
			name:    "can't access map key field",
			md:      md,
			path:    "strkeymap.key",
			wantErr: "map internal field \"key\" may not be traversed",
		},
		{
			name:    "can't access map value field",
			md:      md,
			path:    "strkeymap.value",
			wantErr: "map internal field \"value\" may not be traversed",
		},
		{
			name:    "huge list index",
			md:      md,
			path:    "repeats[0xfffffffffffffffff]",
			wantErr: "non-integral type 0xfffffffffffffffff", // doesn't fit in any integral type.
		},
		{
			name:    "ident index",
			md:      md,
			path:    "repeats[x]",
			wantErr: "expected value for index, not an identifier \"x\"",
		},
		{
			name:    "root[",
			md:      md,
			path:    "(testprotopath.Test)[5]",
			wantErr: "expect either '.' or eof following root",
		},
		{
			name:    "[",
			md:      md,
			path:    "[5]",
			wantErr: "expect first field or '(' for full name",
		},
		{
			name:    "dot index",
			md:      md,
			path:    "int32repeats[.5]",
			wantErr: "got '.'",
		},
		{
			name:    "bool index",
			md:      md,
			path:    "int32repeats[false]",
			wantErr: "non-integral type false", // bool is integral, but won't be cast to 0 or 1.
		},
		{
			name:    "whitespace is illegal",
			md:      md,
			path:    " ",
			wantErr: "found illegal token ' ' at position 0",
		},
		{
			name:    "unexpected identifier",
			md:      md,
			path:    "strkeymap['foo'bar]",
			wantErr: "got identifier \"bar\"",
		},
		{
			name:    "unindexable",
			md:      md,
			path:    "nested.stringfield[0]",
			wantErr: "expected field descriptor with repeated cardinality",
		},
		{
			name:    "weird (",
			md:      md,
			path:    "nested(testprotopath.Test)",
			wantErr: "got '('",
		},
		{
			name:    "bad token",
			md:      md,
			path:    "'",
			wantErr: "found illegal token ' at position 1",
		},
		{
			name:    "bad token after good",
			md:      md,
			path:    "nested🎉",
			wantErr: "found illegal token '🎉'",
		},
		// ---- list wildcard ----
		{
			name: "wildcard star",
			md:   md,
			path: "repeats[*]",
			want: "(testprotopath.Test).repeats[*]",
		},
		{
			name: "wildcard colon",
			md:   md,
			path: "repeats[:]",
			want: "(testprotopath.Test).repeats[*]", // [:] normalizes to [*]
		},
		{
			name:    "wildcard on map",
			md:      md,
			path:    "strkeymap[*]",
			wantErr: "wildcard not supported on map fields",
		},
		{
			name:    "colon wildcard on map",
			md:      md,
			path:    "strkeymap[:]",
			wantErr: "range notation not supported on map fields",
		},
		// ---- list range ----
		{
			name: "range closed",
			md:   md,
			path: "repeats[0:3]",
			want: "(testprotopath.Test).repeats[0:3]",
		},
		{
			name: "range open",
			md:   md,
			path: "repeats[2:]",
			want: "(testprotopath.Test).repeats[2:]",
		},
		{
			name: "range negative start open",
			md:   md,
			path: "repeats[-2:]",
			want: "(testprotopath.Test).repeats[-2:]",
		},
		{
			name: "range negative start closed",
			md:   md,
			path: "repeats[-3:-1]",
			want: "(testprotopath.Test).repeats[-3:-1]",
		},
		{
			name: "range inverted",
			md:   md,
			path: "repeats[5:2]",
			want: "(testprotopath.Test).repeats[5:2]", // valid with positive step, empty at traversal time
		},
		{
			name: "range inverted negative allowed",
			md:   md,
			path: "repeats[-1:-3]",
			want: "(testprotopath.Test).repeats[-1:-3]", // negative: no parse-time check
		},
		{
			name:    "range on map field",
			md:      md,
			path:    "strkeymap[0:3]",
			wantErr: "cannot index map with key kind string with key 0",
		},
		// ---- range and wildcard combined with field access ----
		{
			name: "wildcard then field",
			md:   md,
			path: "repeats[*].nested.stringfield",
			want: "(testprotopath.Test).repeats[*].nested.stringfield",
		},
		{
			name: "range then field",
			md:   md,
			path: "repeats[0:2].nested.stringfield",
			want: "(testprotopath.Test).repeats[0:2].nested.stringfield",
		},
		{
			name: "scalar repeated wildcard",
			md:   md,
			path: "int32repeats[*]",
			want: "(testprotopath.Test).int32repeats[*]",
		},
		{
			name: "scalar repeated range",
			md:   md,
			path: "int32repeats[1:3]",
			want: "(testprotopath.Test).int32repeats[1:3]",
		},
		// ---- colon-only range starting from 0 ----
		{
			name: "colon with end",
			md:   md,
			path: "repeats[:2]",
			want: "(testprotopath.Test).repeats[:2]",
		},		// ---- Python-style stride / step ----
		{
			name: "double colon wildcard",
			md:   md,
			path: "repeats[::]",
			want: "(testprotopath.Test).repeats[*]", // [::] normalizes to [*]
		},
		{
			name: "step every other",
			md:   md,
			path: "repeats[::2]",
			want: "(testprotopath.Test).repeats[::2]",
		},
		{
			name: "reverse all",
			md:   md,
			path: "repeats[::-1]",
			want: "(testprotopath.Test).repeats[::-1]",
		},
		{
			name: "start and step",
			md:   md,
			path: "repeats[1::2]",
			want: "(testprotopath.Test).repeats[1::2]",
		},
		{
			name: "full slice with step",
			md:   md,
			path: "repeats[0:10:3]",
			want: "(testprotopath.Test).repeats[0:10:3]",
		},
		{
			name: "reverse range",
			md:   md,
			path: "repeats[5:2:-1]",
			want: "(testprotopath.Test).repeats[5:2:-1]",
		},
		{
			name: "end and step only",
			md:   md,
			path: "repeats[:3:2]",
			want: "(testprotopath.Test).repeats[:3:2]",
		},
		{
			name: "step with explicit 1",
			md:   md,
			path: "repeats[0:3:1]",
			want: "(testprotopath.Test).repeats[0:3]", // step=1 not printed
		},
		{
			name: "double colon with 1",
			md:   md,
			path: "repeats[::1]",
			want: "(testprotopath.Test).repeats[*]", // [::1] normalizes to [*]
		},
		{
			name:    "step zero error",
			md:      md,
			path:    "repeats[::0]",
			wantErr: "step must not be zero",
		},
		{
			name: "negative step negative bounds",
			md:   md,
			path: "repeats[-1:-4:-1]",
			want: "(testprotopath.Test).repeats[-1:-4:-1]",
		},
		{
			name: "scalar repeated stride",
			md:   md,
			path: "int32repeats[::2]",
			want: "(testprotopath.Test).int32repeats[::2]",
		},	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParsePath(tc.md, tc.path)
			if !matchErr(err, tc.wantErr) {
				t.Fatalf("ParsePath(%q) = %v, %v errored unexpectedly. Want %q", tc.path, got, err, tc.wantErr)
			}
			if tc.wantErr != "" {
				return
			}
			if diff := cmp.Diff(tc.want, got.String()); diff != "" {
				t.Fatalf("ParsePath(%q) diff (-want +got) %s", tc.path, diff)
			}
		})
	}
}
