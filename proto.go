package bufarrowlib

import (
	"context"
	"fmt"

	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

// CompileProtoToFileDescriptor compiles a .proto file at runtime using
// protocompile and returns the resulting FileDescriptor. importPaths are the
// directories searched for transitive imports.
func CompileProtoToFileDescriptor(protoFilePath string, importPaths []string) (protoreflect.FileDescriptor, error) {
	compiler := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{
			// Roots to search for imports; add any import paths your .proto files need
			ImportPaths: importPaths,
		}),
	}

	// Compile returns a linker.Files; each element implements protoreflect.FileDescriptor
	files, err := compiler.Compile(context.Background(), protoFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to compile proto file: %w", err)
	}

	return files[0], nil // Return the first file descriptor (assuming protoFilePath is unique)
}

// GetMessageDescriptorByName looks up a top-level message by name in the
// given FileDescriptor. Returns an error if the message is not found.
func GetMessageDescriptorByName(fd protoreflect.FileDescriptor, messageName string) (protoreflect.MessageDescriptor, error) {
	msgs := fd.Messages()
	for i := 0; i < msgs.Len(); i++ {
		if string(msgs.Get(i).Name()) == messageName {
			return msgs.Get(i), nil
		}
	}
	return nil, fmt.Errorf("message %s not found in file descriptor", messageName)
}

// MergeMessageDescriptors merges two message descriptors into a new one with the specified name.
// It appends the fields from the second descriptor to the first, avoiding name conflicts.
// Field numbers from b are auto-renumbered starting after a's max field number to prevent
// wire-format collisions. Nested message types and enum types from b are also carried over.
// The resulting message descriptor is wrapped in a synthetic file descriptor to ensure it
// can be used independently.
func MergeMessageDescriptors(a, b protoreflect.MessageDescriptor, newName string) (protoreflect.MessageDescriptor, error) {
	// Convert both to mutable DescriptorProto
	dpA := protodesc.ToDescriptorProto(a)
	dpB := protodesc.ToDescriptorProto(b)

	// Find the maximum field number in a
	var maxFieldNum int32
	for _, f := range dpA.GetField() {
		if f.GetNumber() > maxFieldNum {
			maxFieldNum = f.GetNumber()
		}
	}

	// Check for field name conflicts
	seenNames := make(map[string]bool)
	for _, f := range dpA.GetField() {
		seenNames[f.GetName()] = true
	}

	// Merge fields from b into a, auto-renumbering to avoid field number collisions
	for _, f := range dpB.GetField() {
		if seenNames[f.GetName()] {
			return nil, fmt.Errorf("field name conflict: %s", f.GetName())
		}
		cloned := proto.Clone(f).(*descriptorpb.FieldDescriptorProto)
		maxFieldNum++
		cloned.Number = proto.Int32(maxFieldNum)
		dpA.Field = append(dpA.Field, cloned)
	}

	// Check for nested message type name conflicts and merge
	seenNestedMsgs := make(map[string]bool)
	for _, nt := range dpA.GetNestedType() {
		seenNestedMsgs[nt.GetName()] = true
	}
	for _, nt := range dpB.GetNestedType() {
		if seenNestedMsgs[nt.GetName()] {
			return nil, fmt.Errorf("nested message type name conflict: %s", nt.GetName())
		}
		dpA.NestedType = append(dpA.NestedType, proto.Clone(nt).(*descriptorpb.DescriptorProto))
	}

	// Check for enum type name conflicts and merge
	seenEnums := make(map[string]bool)
	for _, et := range dpA.GetEnumType() {
		seenEnums[et.GetName()] = true
	}
	for _, et := range dpB.GetEnumType() {
		if seenEnums[et.GetName()] {
			return nil, fmt.Errorf("enum type name conflict: %s", et.GetName())
		}
		dpA.EnumType = append(dpA.EnumType, proto.Clone(et).(*descriptorpb.EnumDescriptorProto))
	}

	dpA.Name = proto.String(newName)

	// Collect all direct and transitive dependency file paths for both parent
	// files. The merged synthetic file inherits all field type references from
	// both parents, so it must explicitly declare every file containing a
	// referenced type — not just the top-level parent files.
	depSet := make(map[string]bool)
	var deps []string

	var addFileDeps func(fd protoreflect.FileDescriptor)
	addFileDeps = func(fd protoreflect.FileDescriptor) {
		p := string(fd.Path())
		if depSet[p] {
			return
		}
		depSet[p] = true
		deps = append(deps, p)
		for i := 0; i < fd.Imports().Len(); i++ {
			addFileDeps(fd.Imports().Get(i))
		}
	}
	addFileDeps(a.ParentFile())
	if b.ParentFile().Path() != a.ParentFile().Path() {
		addFileDeps(b.ParentFile())
	}

	fdp := &descriptorpb.FileDescriptorProto{
		Name:        proto.String(newName + ".proto"),
		Syntax:      proto.String("proto3"),
		Dependency:  deps,
		MessageType: []*descriptorpb.DescriptorProto{dpA},
	}

	// Build resolver from the already-known parent files, including all
	// transitive imports so that well-known types (e.g. google.protobuf.Timestamp)
	// and any other indirect dependencies resolve successfully.
	resolver := &protoregistry.Files{}
	if err := registerFileTransitively(resolver, a.ParentFile()); err != nil {
		return nil, fmt.Errorf("failed to register parent file of a: %w", err)
	}
	if b.ParentFile().Path() != a.ParentFile().Path() {
		if err := registerFileTransitively(resolver, b.ParentFile()); err != nil {
			return nil, fmt.Errorf("failed to register parent file of b: %w", err)
		}
	}
	// Build a real FileDescriptor from it
	fd, err := protodesc.NewFile(fdp, resolver)
	if err != nil {
		return nil, fmt.Errorf("failed to create file descriptor with resolver: %w", err)
	}

	return fd.Messages().Get(0), nil
}

// registerFileTransitively registers fd and all of its transitive imports into r.
// Already-registered files are skipped so repeated calls are safe.
func registerFileTransitively(r *protoregistry.Files, fd protoreflect.FileDescriptor) error {
	// Depth-first: register imports before the file that depends on them.
	for i := 0; i < fd.Imports().Len(); i++ {
		imp := fd.Imports().Get(i)
		if _, err := r.FindFileByPath(string(imp.Path())); err == nil {
			continue // already registered
		}
		if err := registerFileTransitively(r, imp); err != nil {
			return err
		}
	}
	if _, err := r.FindFileByPath(string(fd.Path())); err != nil {
		if err := r.RegisterFile(fd); err != nil {
			return err
		}
	}
	return nil
}
