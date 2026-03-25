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
// repeated Test field called "repeats".
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

// ExampleNewPlan demonstrates compiling multiple paths into a Plan for
// repeated evaluation against messages of the same type.
func ExampleNewPlan() {
	testMD, nestedMD := exampleDescriptors()

	// Compile two paths — they share the "repeats[*].nested" prefix so
	// the Plan traverses it only once.
	plan, err := pbpath.NewPlan(testMD,
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

// ExamplePlan_Eval shows how fan-out paths produce multiple Values branches.
func ExamplePlan_Eval() {
	testMD, nestedMD := exampleDescriptors()

	plan, err := pbpath.NewPlan(testMD,
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

// ExamplePlan_Entries shows how to inspect a Plan's compiled entries.
func ExamplePlan_Entries() {
	testMD, _ := exampleDescriptors()

	plan, err := pbpath.NewPlan(testMD,
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
// multi-path evaluation.
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
