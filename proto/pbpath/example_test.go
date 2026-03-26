package pbpath_test

import (
	"fmt"
	"log"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/loicalleyne/bufarrowlib/proto/pbpath"
)

// exampleDescriptors constructs a Test schema with a Nested submessage and a
// repeated Test field called "repeats". This models a self-referential proto
// message:
//
//	message Test {
//	  Nested           nested  = 1;
//	  repeated Test    repeats = 2;
//	  message Nested {
//	    string stringfield = 1;
//	  }
//	}
//
// Self-referential (recursive) messages are fully supported by pbpath.
func exampleDescriptors() (protoreflect.MessageDescriptor, protoreflect.MessageDescriptor) {
	stringType := descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
	messageType := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
	labelOptional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
	labelRepeated := descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()

	fdp := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("example.proto"),
		Package: proto.String("example"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("Test"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{Name: proto.String("nested"), Number: proto.Int32(1), Type: messageType, TypeName: proto.String(".example.Test.Nested"), Label: labelOptional},
					{Name: proto.String("repeats"), Number: proto.Int32(2), Type: messageType, TypeName: proto.String(".example.Test"), Label: labelRepeated},
				},
				NestedType: []*descriptorpb.DescriptorProto{
					{
						Name: proto.String("Nested"),
						Field: []*descriptorpb.FieldDescriptorProto{
							{Name: proto.String("stringfield"), Number: proto.Int32(1), Type: stringType, Label: labelOptional},
						},
					},
				},
			},
		},
	}
	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		log.Fatalf("protodesc.NewFile: %v", err)
	}
	testMD := fd.Messages().ByName("Test")
	nestedMD := testMD.Messages().ByName("Nested")
	return testMD, nestedMD
}

// ExampleParsePath demonstrates parsing a path string against a message
// descriptor. The returned Path is a slice of Step values that can be
// inspected or passed to PathValues for traversal.
func ExampleParsePath() {
	testMD, _ := exampleDescriptors()

	// Parse a simple field access path.
	path, err := pbpath.ParsePath(testMD, "nested.stringfield")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("steps: %d\n", len(path))
	fmt.Printf("path:  %s\n", path)

	// Parse a wildcard path — fans out across all elements of "repeats".
	path2, err := pbpath.ParsePath(testMD, "repeats[*].nested.stringfield")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("fan-out path: %s\n", path2)

	// Output:
	// steps: 3
	// path:  (example.Test).nested.stringfield
	// fan-out path: (example.Test).repeats[*].nested.stringfield
}

// ExamplePathValues demonstrates traversing a path through a live message
// to extract values. For scalar paths, exactly one Values is returned.
func ExamplePathValues() {
	testMD, nestedMD := exampleDescriptors()

	// Build a message: Test { nested: { stringfield: "hello" } }
	nested := dynamicpb.NewMessage(nestedMD)
	nested.Set(nestedMD.Fields().ByName("stringfield"), protoreflect.ValueOfString("hello"))
	msg := dynamicpb.NewMessage(testMD)
	msg.Set(testMD.Fields().ByName("nested"), protoreflect.ValueOfMessage(nested))

	// Parse and evaluate.
	path, _ := pbpath.ParsePath(testMD, "nested.stringfield")
	results, err := pbpath.PathValues(path, msg)
	if err != nil {
		log.Fatal(err)
	}

	// Scalar path → exactly one result.
	fmt.Printf("branches: %d\n", len(results))

	// Index(-1) returns the last (step, value) pair — the leaf value.
	leaf := results[0].Index(-1)
	fmt.Printf("value: %s\n", leaf.Value.String())

	// Output:
	// branches: 1
	// value: hello
}

// ExamplePathValues_fanout demonstrates how wildcards cause PathValues to
// produce multiple result branches — one per matching list element.
func ExamplePathValues_fanout() {
	testMD, nestedMD := exampleDescriptors()

	// Build a message with 3 elements in the "repeats" list.
	msg := dynamicpb.NewMessage(testMD)
	list := msg.Mutable(testMD.Fields().ByName("repeats")).List()
	for _, v := range []string{"alpha", "beta", "gamma"} {
		n := dynamicpb.NewMessage(nestedMD)
		n.Set(nestedMD.Fields().ByName("stringfield"), protoreflect.ValueOfString(v))
		child := dynamicpb.NewMessage(testMD)
		child.Set(testMD.Fields().ByName("nested"), protoreflect.ValueOfMessage(n))
		list.Append(protoreflect.ValueOfMessage(child))
	}

	path, _ := pbpath.ParsePath(testMD, "repeats[*].nested.stringfield")
	results, err := pbpath.PathValues(path, msg)
	if err != nil {
		log.Fatal(err)
	}

	// One branch per list element, each with the concrete index resolved.
	fmt.Printf("branches: %d\n", len(results))
	for _, r := range results {
		// ListIndices() extracts the concrete list indices visited.
		indices := r.ListIndices()
		fmt.Printf("  repeats[%d] = %s\n", indices[0], r.Index(-1).Value.String())
	}

	// Output:
	// branches: 3
	//   repeats[0] = alpha
	//   repeats[1] = beta
	//   repeats[2] = gamma
}

// ExampleNewPlan demonstrates compiling multiple paths into a Plan for
// repeated evaluation against messages of the same type.
//
// The Plan API is the recommended approach for hot paths because:
//   - Paths sharing a common prefix are traversed only once (trie merging).
//   - EvalLeaves reuses scratch buffers, eliminating per-call allocations.
//   - The Plan is immutable and safe for concurrent use (with EvalLeavesConcurrent).
func ExampleNewPlan() {
	testMD, nestedMD := exampleDescriptors()

	// Compile two paths — they share the "repeats[*].nested" prefix so
	// the Plan traverses it only once.
	plan, err := pbpath.NewPlan(testMD, nil,
		pbpath.PlanPath("repeats[*].nested.stringfield", pbpath.Alias("name")),
		pbpath.PlanPath("nested.stringfield", pbpath.Alias("top")),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Build a message:
	//   Test { nested: { stringfield: "root" }, repeats: [
	//     Test{ nested: { stringfield: "a" } },
	//     Test{ nested: { stringfield: "b" } },
	//   ]}
	nested := dynamicpb.NewMessage(nestedMD)
	nested.Set(nestedMD.Fields().ByName("stringfield"), protoreflect.ValueOfString("root"))
	msg := dynamicpb.NewMessage(testMD)
	msg.Set(testMD.Fields().ByName("nested"), protoreflect.ValueOfMessage(nested))
	list := msg.Mutable(testMD.Fields().ByName("repeats")).List()
	for _, v := range []string{"a", "b"} {
		n := dynamicpb.NewMessage(nestedMD)
		n.Set(nestedMD.Fields().ByName("stringfield"), protoreflect.ValueOfString(v))
		child := dynamicpb.NewMessage(testMD)
		child.Set(testMD.Fields().ByName("nested"), protoreflect.ValueOfMessage(n))
		list.Append(protoreflect.ValueOfMessage(child))
	}

	// Evaluate.
	results, err := plan.Eval(msg)
	if err != nil {
		log.Fatal(err)
	}

	// results[0] = "name" path (repeats[*].nested.stringfield) → 2 branches
	fmt.Printf("name: %d branches\n", len(results[0]))
	for _, v := range results[0] {
		fmt.Printf("  %v\n", v.Index(-1).Value.Interface())
	}
	// results[1] = "top" path (nested.stringfield) → 1 branch
	fmt.Printf("top: %v\n", results[1][0].Index(-1).Value.Interface())

	// Output:
	// name: 2 branches
	//   a
	//   b
	// top: root
}

// ExamplePlan_EvalLeaves demonstrates the high-performance EvalLeaves method,
// which returns only leaf values (not full path chains) and reuses internal
// scratch buffers to minimize allocations.
//
// Use EvalLeaves for hot paths where you process thousands of messages per
// second. For concurrent access from multiple goroutines, use
// EvalLeavesConcurrent instead.
func ExamplePlan_EvalLeaves() {
	testMD, nestedMD := exampleDescriptors()

	plan, err := pbpath.NewPlan(testMD, nil,
		pbpath.PlanPath("nested.stringfield", pbpath.Alias("top_name")),
		pbpath.PlanPath("repeats[*].nested.stringfield", pbpath.Alias("child_name")),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Build a message with nested value and two repeats.
	nested := dynamicpb.NewMessage(nestedMD)
	nested.Set(nestedMD.Fields().ByName("stringfield"), protoreflect.ValueOfString("root"))
	msg := dynamicpb.NewMessage(testMD)
	msg.Set(testMD.Fields().ByName("nested"), protoreflect.ValueOfMessage(nested))
	list := msg.Mutable(testMD.Fields().ByName("repeats")).List()
	for _, v := range []string{"x", "y"} {
		n := dynamicpb.NewMessage(nestedMD)
		n.Set(nestedMD.Fields().ByName("stringfield"), protoreflect.ValueOfString(v))
		child := dynamicpb.NewMessage(testMD)
		child.Set(testMD.Fields().ByName("nested"), protoreflect.ValueOfMessage(n))
		list.Append(protoreflect.ValueOfMessage(child))
	}

	// EvalLeaves returns [][]protoreflect.Value — just the leaf values.
	// No full path chains, no per-call allocations (scratch buffers reused).
	leaves, err := plan.EvalLeaves(msg)
	if err != nil {
		log.Fatal(err)
	}

	// leaves[0] = top_name → 1 value
	fmt.Printf("top_name: %s\n", leaves[0][0].String())

	// leaves[1] = child_name → 2 values (one per repeat)
	fmt.Printf("child_names: %d values\n", len(leaves[1]))
	for _, v := range leaves[1] {
		fmt.Printf("  %s\n", v.String())
	}

	// Output:
	// top_name: root
	// child_names: 2 values
	//   x
	//   y
}

// ExamplePlan_Eval shows how fan-out paths produce multiple Values branches.
// The [0:2] range slice selects the first two elements of the repeated field,
// even though three elements exist.
func ExamplePlan_Eval() {
	testMD, nestedMD := exampleDescriptors()

	plan, err := pbpath.NewPlan(testMD, nil,
		pbpath.PlanPath("repeats[0:2].nested.stringfield"),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Three-element repeated field — the [0:2] slice selects the first two.
	msg := dynamicpb.NewMessage(testMD)
	list := msg.Mutable(testMD.Fields().ByName("repeats")).List()
	for _, v := range []string{"x", "y", "z"} {
		n := dynamicpb.NewMessage(nestedMD)
		n.Set(nestedMD.Fields().ByName("stringfield"), protoreflect.ValueOfString(v))
		child := dynamicpb.NewMessage(testMD)
		child.Set(testMD.Fields().ByName("nested"), protoreflect.ValueOfMessage(n))
		list.Append(protoreflect.ValueOfMessage(child))
	}

	results, err := plan.Eval(msg)
	if err != nil {
		log.Fatal(err)
	}

	for _, v := range results[0] {
		fmt.Println(v.Index(-1).Value.Interface())
	}
	// Output:
	// x
	// y
}

// ExamplePlan_Entries shows how to inspect a Plan's compiled entries. This is
// useful for mapping result slots to output column names — each entry's Name
// is the alias (if provided) or the raw path string.
func ExamplePlan_Entries() {
	testMD, _ := exampleDescriptors()

	plan, err := pbpath.NewPlan(testMD, nil,
		pbpath.PlanPath("nested.stringfield", pbpath.Alias("col_a")),
		pbpath.PlanPath("repeats[*].nested.stringfield"),
	)
	if err != nil {
		log.Fatal(err)
	}

	for _, e := range plan.Entries() {
		fmt.Printf("%-30s  path: %s\n", e.Name, e.Path)
	}
	// Output:
	// col_a                           path: (example.Test).nested.stringfield
	// repeats[*].nested.stringfield   path: (example.Test).repeats[*].nested.stringfield
}

// ExamplePathValuesMulti demonstrates the convenience wrapper for one-shot
// multi-path evaluation. This is useful for tests and ad-hoc extractions.
// For repeated evaluation of the same paths across many messages, prefer
// NewPlan + EvalLeaves.
func ExamplePathValuesMulti() {
	testMD, nestedMD := exampleDescriptors()

	nested := dynamicpb.NewMessage(nestedMD)
	nested.Set(nestedMD.Fields().ByName("stringfield"), protoreflect.ValueOfString("hello"))
	msg := dynamicpb.NewMessage(testMD)
	msg.Set(testMD.Fields().ByName("nested"), protoreflect.ValueOfMessage(nested))

	results, err := pbpath.PathValuesMulti(testMD, msg,
		pbpath.PlanPath("nested.stringfield", pbpath.Alias("greeting")),
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(results[0][0].Index(-1).Value.Interface())
	// Output:
	// hello
}

// ExampleStrictPath demonstrates using StrictPath to detect when a range or
// index was clamped because the list is shorter than expected. Without
// StrictPath, out-of-bounds accesses are silently skipped.
func ExampleStrictPath() {
	testMD, nestedMD := exampleDescriptors()

	plan, err := pbpath.NewPlan(testMD, nil,
		// This path expects at least 10 elements — will error if fewer exist.
		pbpath.PlanPath("repeats[0:10].nested.stringfield", pbpath.StrictPath()),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Build a message with only 2 repeats — the range [0:10] will be clamped.
	msg := dynamicpb.NewMessage(testMD)
	list := msg.Mutable(testMD.Fields().ByName("repeats")).List()
	for _, v := range []string{"a", "b"} {
		n := dynamicpb.NewMessage(nestedMD)
		n.Set(nestedMD.Fields().ByName("stringfield"), protoreflect.ValueOfString(v))
		child := dynamicpb.NewMessage(testMD)
		child.Set(testMD.Fields().ByName("nested"), protoreflect.ValueOfMessage(n))
		list.Append(protoreflect.ValueOfMessage(child))
	}

	_, err = plan.Eval(msg)
	fmt.Printf("strict error: %v\n", err != nil)

	// Output:
	// strict error: true
}

