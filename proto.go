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

// PruneEmptyMessages traverses the complete field tree of md and removes any
// field whose message type has no fields (directly or after cascading removal).
// The pruning is recursive and bottom-up: if removing empty-message fields
// causes an ancestor message to also become empty, that ancestor's fields are
// pruned as well. Map entry messages are never considered empty (they always
// carry key and value fields).
//
// Field numbers of surviving fields are left unchanged to preserve wire-format
// compatibility. Orphaned oneof declarations (where every variant was removed)
// are cleaned up and the remaining fields' oneof indices are adjusted.
//
// Since protoreflect.MessageDescriptor is immutable, the function converts md
// to a mutable descriptorpb.DescriptorProto, performs the pruning, then
// compiles the result back to a protoreflect.MessageDescriptor using
// protodesc.NewFile.
//
// An error is returned if the resulting top-level message has no fields.
func PruneEmptyMessages(md protoreflect.MessageDescriptor) (protoreflect.MessageDescriptor, error) {
	emptySet := computeEmptyMessages(md)

	dp := protodesc.ToDescriptorProto(md)
	pruneDescriptorProto(dp, emptySet)

	if len(dp.GetField()) == 0 {
		return nil, fmt.Errorf("PruneEmptyMessages: all fields removed from message %s", md.FullName())
	}

	// Collect transitive dependencies from the parent file.
	depSet := make(map[string]bool)
	var deps []string
	var collectDeps func(f protoreflect.FileDescriptor)
	collectDeps = func(f protoreflect.FileDescriptor) {
		p := string(f.Path())
		if depSet[p] {
			return
		}
		depSet[p] = true
		deps = append(deps, p)
		for i := 0; i < f.Imports().Len(); i++ {
			collectDeps(f.Imports().Get(i))
		}
	}
	collectDeps(md.ParentFile())

	fdp := &descriptorpb.FileDescriptorProto{
		Name:        proto.String(string(md.Name()) + "_pruned.proto"),
		Package:     proto.String(string(md.ParentFile().Package())),
		Syntax:      proto.String("proto3"),
		Dependency:  deps,
		MessageType: []*descriptorpb.DescriptorProto{dp},
	}

	resolver := &protoregistry.Files{}
	if err := registerFileTransitively(resolver, md.ParentFile()); err != nil {
		return nil, fmt.Errorf("PruneEmptyMessages: failed to register parent file: %w", err)
	}

	fd, err := protodesc.NewFile(fdp, resolver)
	if err != nil {
		return nil, fmt.Errorf("PruneEmptyMessages: failed to create file descriptor: %w", err)
	}

	return fd.Messages().Get(0), nil
}

// computeEmptyMessages returns the set of fully-qualified message names that
// are "effectively empty" — i.e. they have zero non-empty fields after
// recursively accounting for fields whose message types are themselves
// effectively empty. Map entry messages are never considered empty.
//
// The algorithm performs a fixed-point iteration: seed with messages that
// have zero declared fields, then repeatedly promote messages whose every
// message-typed field resolves to an already-confirmed-empty name, until no
// new entries are added.
func computeEmptyMessages(md protoreflect.MessageDescriptor) map[protoreflect.FullName]bool {
	// Collect all reachable message descriptors via DFS.
	all := map[protoreflect.FullName]protoreflect.MessageDescriptor{}
	var collect func(m protoreflect.MessageDescriptor)
	collect = func(m protoreflect.MessageDescriptor) {
		name := m.FullName()
		if _, seen := all[name]; seen {
			return
		}
		all[name] = m
		// Recurse into nested message declarations.
		nested := m.Messages()
		for i := 0; i < nested.Len(); i++ {
			collect(nested.Get(i))
		}
		// Recurse into message types referenced by fields.
		fields := m.Fields()
		for i := 0; i < fields.Len(); i++ {
			f := fields.Get(i)
			if f.Kind() == protoreflect.MessageKind || f.Kind() == protoreflect.GroupKind {
				collect(f.Message())
			}
		}
	}
	collect(md)

	empty := map[protoreflect.FullName]bool{}

	// Seed: messages that already have zero declared fields (and are not map entries).
	for name, m := range all {
		if m.Fields().Len() == 0 && !m.IsMapEntry() {
			empty[name] = true
		}
	}

	// Fixed-point: promote messages whose every message-typed field points to
	// an effectively-empty message, leaving zero non-empty fields.
	for {
		changed := false
		for name, m := range all {
			if empty[name] {
				continue
			}
			if m.IsMapEntry() {
				continue
			}
			nonEmpty := 0
			fields := m.Fields()
			for i := 0; i < fields.Len(); i++ {
				f := fields.Get(i)
				if f.IsMap() {
					// Map fields are always non-empty.
					nonEmpty++
					continue
				}
				if f.Kind() == protoreflect.MessageKind || f.Kind() == protoreflect.GroupKind {
					if !empty[f.Message().FullName()] {
						nonEmpty++
					}
				} else {
					// Scalar / enum fields are always non-empty.
					nonEmpty++
				}
			}
			if nonEmpty == 0 {
				empty[name] = true
				changed = true
			}
		}
		if !changed {
			break
		}
	}

	return empty
}

// pruneDescriptorProto removes from dp (and its NestedType descendants)
// any field whose message type appears in emptySet. Orphaned oneof
// declarations are removed and remaining fields' OneofIndex values are
// adjusted. Map entry nested types are never removed.
//
// emptySet keys are fully-qualified names without a leading dot, matching
// protoreflect.FullName (e.g. "mypkg.MyMessage").
func pruneDescriptorProto(dp *descriptorpb.DescriptorProto, emptySet map[protoreflect.FullName]bool) {
	// Recurse into nested types first (bottom-up).
	for _, nested := range dp.GetNestedType() {
		if nested.GetOptions().GetMapEntry() {
			continue
		}
		pruneDescriptorProto(nested, emptySet)
	}

	// Filter fields: remove those whose TypeName resolves to an empty message.
	filtered := dp.Field[:0]
	for _, f := range dp.Field {
		if f.GetType() == descriptorpb.FieldDescriptorProto_TYPE_MESSAGE ||
			f.GetType() == descriptorpb.FieldDescriptorProto_TYPE_GROUP {
			// TypeName is ".pkg.Msg"; trim the leading dot for emptySet lookup.
			typeName := f.GetTypeName()
			if len(typeName) > 0 && typeName[0] == '.' {
				typeName = typeName[1:]
			}
			if emptySet[protoreflect.FullName(typeName)] {
				continue // drop this field
			}
		}
		filtered = append(filtered, f)
	}
	dp.Field = filtered

	// Clean up orphaned oneof declarations. A oneof is orphaned when every
	// field that referenced it was removed. Build a set of oneof indices that
	// still have at least one surviving field.
	activeOneof := map[int32]bool{}
	for _, f := range dp.Field {
		if f.OneofIndex != nil {
			activeOneof[f.GetOneofIndex()] = true
		}
	}

	if len(activeOneof) == len(dp.OneofDecl) {
		// No orphans — nothing to do.
		return
	}

	// Build a remapping: old oneof index → new oneof index (after compaction).
	newDecls := dp.OneofDecl[:0]
	indexRemap := map[int32]int32{}
	for oldIdx, decl := range dp.OneofDecl {
		if activeOneof[int32(oldIdx)] {
			newIdx := int32(len(newDecls))
			indexRemap[int32(oldIdx)] = newIdx
			newDecls = append(newDecls, decl)
		}
	}
	dp.OneofDecl = newDecls

	// Update surviving fields' OneofIndex to the remapped values.
	for _, f := range dp.Field {
		if f.OneofIndex != nil {
			newIdx := indexRemap[f.GetOneofIndex()]
			f.OneofIndex = proto.Int32(newIdx)
		}
	}
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
