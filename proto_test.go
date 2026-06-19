package bufarrowlib

import (
	"path/filepath"
	"runtime"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// testProtoDir returns the absolute path to the proto/samples directory.
func testProtoDir(t *testing.T) string {
	t.Helper()
	_, f, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("unable to determine test file path")
	}
	return filepath.Join(filepath.Dir(f), "proto", "samples")
}

func TestCompileProtoToFileDescriptor(t *testing.T) {
	protoDir := testProtoDir(t)

	t.Run("success", func(t *testing.T) {
		fd, err := CompileProtoToFileDescriptor("samples.proto", []string{protoDir})
		if err != nil {
			t.Fatalf("CompileProtoToFileDescriptor() error = %v", err)
		}
		if fd == nil {
			t.Fatal("CompileProtoToFileDescriptor() returned nil file descriptor")
		}
		// Verify it found the expected messages
		msgs := fd.Messages()
		if msgs.Len() == 0 {
			t.Fatal("expected at least one message in the file descriptor")
		}
		// Check a known message exists
		found := false
		for i := 0; i < msgs.Len(); i++ {
			if string(msgs.Get(i).Name()) == "ScalarTypes" {
				found = true
				break
			}
		}
		if !found {
			t.Error("expected to find message ScalarTypes in the compiled file descriptor")
		}
	})

	t.Run("invalid_proto_file", func(t *testing.T) {
		_, err := CompileProtoToFileDescriptor("nonexistent.proto", []string{"."})
		if err == nil {
			t.Fatal("expected error for nonexistent proto file, got nil")
		}
	})

	t.Run("invalid_import_path", func(t *testing.T) {
		// samples.proto imports google/protobuf/timestamp.proto;
		// providing an empty import path list still works because
		// protocompile WithStandardImports covers well-known types.
		// But a completely bogus source file should still fail.
		_, err := CompileProtoToFileDescriptor("nonexistent.proto", []string{"/no/such/path"})
		if err == nil {
			t.Fatal("expected error for invalid import path, got nil")
		}
	})
}

func TestGetMessageDescriptorByName(t *testing.T) {
	protoDir := testProtoDir(t)

	fd, err := CompileProtoToFileDescriptor("samples.proto", []string{protoDir})
	if err != nil {
		t.Fatalf("setup: CompileProtoToFileDescriptor() error = %v", err)
	}

	t.Run("existing_message", func(t *testing.T) {
		md, err := GetMessageDescriptorByName(fd, "ScalarTypes")
		if err != nil {
			t.Fatalf("GetMessageDescriptorByName() error = %v", err)
		}
		if md == nil {
			t.Fatal("GetMessageDescriptorByName() returned nil")
		}
		if string(md.Name()) != "ScalarTypes" {
			t.Errorf("expected message name ScalarTypes, got %s", md.Name())
		}
	})

	t.Run("another_existing_message", func(t *testing.T) {
		md, err := GetMessageDescriptorByName(fd, "Nested")
		if err != nil {
			t.Fatalf("GetMessageDescriptorByName() error = %v", err)
		}
		if string(md.Name()) != "Nested" {
			t.Errorf("expected message name Nested, got %s", md.Name())
		}
	})

	t.Run("nonexistent_message", func(t *testing.T) {
		_, err := GetMessageDescriptorByName(fd, "DoesNotExist")
		if err == nil {
			t.Fatal("expected error for nonexistent message, got nil")
		}
	})

	t.Run("fields_are_present", func(t *testing.T) {
		md, err := GetMessageDescriptorByName(fd, "ScalarTypes")
		if err != nil {
			t.Fatalf("GetMessageDescriptorByName() error = %v", err)
		}
		fields := md.Fields()
		if fields.Len() != 15 {
			t.Errorf("expected 15 fields in ScalarTypes, got %d", fields.Len())
		}
	})
}

func TestMergeMessageDescriptors(t *testing.T) {
	protoDir := testProtoDir(t)

	// Use merge_test.proto which has scalar-only messages with non-overlapping fields
	fd, err := CompileProtoToFileDescriptor("merge_test.proto", []string{protoDir})
	if err != nil {
		t.Fatalf("setup: CompileProtoToFileDescriptor() error = %v", err)
	}

	t.Run("merge_disjoint_messages", func(t *testing.T) {
		// MergeA has: name(1), age(2)
		// MergeB has: score(3), active(4)
		// No field name or number conflicts
		mdA, err := GetMessageDescriptorByName(fd, "MergeA")
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		mdB, err := GetMessageDescriptorByName(fd, "MergeB")
		if err != nil {
			t.Fatalf("setup: %v", err)
		}

		merged, err := MergeMessageDescriptors(mdA, mdB, "MergedAB")
		if err != nil {
			t.Fatalf("MergeMessageDescriptors() error = %v", err)
		}
		if merged == nil {
			t.Fatal("MergeMessageDescriptors() returned nil")
		}
		if string(merged.Name()) != "MergedAB" {
			t.Errorf("expected merged name MergedAB, got %s", merged.Name())
		}
		// Expect fields from both: name, age from MergeA and score, active from MergeB
		fields := merged.Fields()
		if fields.Len() != 4 {
			t.Errorf("expected 4 fields in merged descriptor, got %d", fields.Len())
		}
		fieldNames := make(map[string]bool)
		for i := 0; i < fields.Len(); i++ {
			fieldNames[string(fields.Get(i).Name())] = true
		}
		for _, want := range []string{"name", "age", "score", "active"} {
			if !fieldNames[want] {
				t.Errorf("expected field %q in merged descriptor", want)
			}
		}
	})

	t.Run("merge_with_field_name_conflict", func(t *testing.T) {
		// MergeA has field "name" and MergeC also has field "name"
		mdA, err := GetMessageDescriptorByName(fd, "MergeA")
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		mdC, err := GetMessageDescriptorByName(fd, "MergeC")
		if err != nil {
			t.Fatalf("setup: %v", err)
		}

		_, err = MergeMessageDescriptors(mdA, mdC, "ConflictTest")
		if err == nil {
			t.Fatal("expected field name conflict error, got nil")
		}
	})

	t.Run("merge_self_conflict", func(t *testing.T) {
		// Merging a message with itself should produce a conflict
		mdA, err := GetMessageDescriptorByName(fd, "MergeA")
		if err != nil {
			t.Fatalf("setup: %v", err)
		}

		_, err = MergeMessageDescriptors(mdA, mdA, "SelfConflict")
		if err == nil {
			t.Fatal("expected field name conflict error, got nil")
		}
	})

	t.Run("merged_parent_file_path", func(t *testing.T) {
		mdA, err := GetMessageDescriptorByName(fd, "MergeA")
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		mdB, err := GetMessageDescriptorByName(fd, "MergeB")
		if err != nil {
			t.Fatalf("setup: %v", err)
		}

		merged, err := MergeMessageDescriptors(mdA, mdB, "ParentPathCheck")
		if err != nil {
			t.Fatalf("MergeMessageDescriptors() error = %v", err)
		}
		// The merged descriptor's parent file should have the synthetic name
		parentPath := string(merged.ParentFile().Path())
		if parentPath != "ParentPathCheck.proto" {
			t.Errorf("expected parent file path ParentPathCheck.proto, got %s", parentPath)
		}
	})

	t.Run("merge_same_parent_file", func(t *testing.T) {
		// Both messages come from the same file; ensure the code path
		// that skips double-registering the same parent file works.
		mdA, err := GetMessageDescriptorByName(fd, "MergeA")
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		mdB, err := GetMessageDescriptorByName(fd, "MergeB")
		if err != nil {
			t.Fatalf("setup: %v", err)
		}

		merged, err := MergeMessageDescriptors(mdA, mdB, "SameParentMerge")
		if err != nil {
			t.Fatalf("MergeMessageDescriptors() error = %v", err)
		}
		fields := merged.Fields()
		if fields.Len() != 4 {
			t.Errorf("expected 4 fields, got %d", fields.Len())
		}
	})

	t.Run("merge_different_file_descriptors", func(t *testing.T) {
		// Compile a second proto file to have descriptors from different files
		fd2, err := CompileProtoToFileDescriptor("CustomTypes.proto", []string{filepath.Join(protoDir, "custom")})
		if err != nil {
			t.Fatalf("setup: CompileProtoToFileDescriptor() error = %v", err)
		}

		mdDecimal, err := GetMessageDescriptorByName(fd2, "DecimalValue")
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		// MergeB has fields score(3), active(4) — no overlap with DecimalValue units(1), nanos(2)
		mdB, err := GetMessageDescriptorByName(fd, "MergeB")
		if err != nil {
			t.Fatalf("setup: %v", err)
		}

		merged, err := MergeMessageDescriptors(mdB, mdDecimal, "MergedCrossFile")
		if err != nil {
			t.Fatalf("MergeMessageDescriptors() error = %v", err)
		}
		// MergeB has "score" and "active", DecimalValue has "units" and "nanos"
		fields := merged.Fields()
		if fields.Len() != 4 {
			t.Errorf("expected 4 fields, got %d", fields.Len())
		}
	})

	t.Run("merged_syntax_is_proto3", func(t *testing.T) {
		mdA, _ := GetMessageDescriptorByName(fd, "MergeA")
		mdB, _ := GetMessageDescriptorByName(fd, "MergeB")

		merged, err := MergeMessageDescriptors(mdA, mdB, "SyntaxCheck")
		if err != nil {
			t.Fatalf("MergeMessageDescriptors() error = %v", err)
		}
		syntax := merged.ParentFile().Syntax()
		if syntax != protoreflect.Proto3 {
			t.Errorf("expected proto3 syntax, got %v", syntax)
		}
	})

	t.Run("auto_renumber_fields", func(t *testing.T) {
		// MergeA has: name(1), age(2)
		// MergeB has: score(3), active(4)
		// After merge, B's fields should be renumbered to 3, 4 (starting after A's max=2)
		mdA, _ := GetMessageDescriptorByName(fd, "MergeA")
		mdB, _ := GetMessageDescriptorByName(fd, "MergeB")

		merged, err := MergeMessageDescriptors(mdA, mdB, "RenumberTest")
		if err != nil {
			t.Fatalf("MergeMessageDescriptors() error = %v", err)
		}

		fields := merged.Fields()
		// A's fields keep their original numbers (1, 2)
		nameField := fields.ByName("name")
		if nameField == nil {
			t.Fatal("expected field 'name'")
		}
		if nameField.Number() != 1 {
			t.Errorf("expected name field number 1, got %d", nameField.Number())
		}

		ageField := fields.ByName("age")
		if ageField == nil {
			t.Fatal("expected field 'age'")
		}
		if ageField.Number() != 2 {
			t.Errorf("expected age field number 2, got %d", ageField.Number())
		}

		// B's fields should be renumbered starting after max(A) = 2
		scoreField := fields.ByName("score")
		if scoreField == nil {
			t.Fatal("expected field 'score'")
		}
		if scoreField.Number() != 3 {
			t.Errorf("expected score field number 3, got %d", scoreField.Number())
		}

		activeField := fields.ByName("active")
		if activeField == nil {
			t.Fatal("expected field 'active'")
		}
		if activeField.Number() != 4 {
			t.Errorf("expected active field number 4, got %d", activeField.Number())
		}
	})

	t.Run("auto_renumber_avoids_gaps", func(t *testing.T) {
		// Use messages from different files to test renumbering with different field numbers
		// DecimalValue has: units(1), nanos(2)
		// MergeA has: name(1), age(2)
		// After merge, B's fields should be renumbered to 3, 4
		fd2, err := CompileProtoToFileDescriptor("CustomTypes.proto", []string{filepath.Join(protoDir, "custom")})
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		mdDecimal, _ := GetMessageDescriptorByName(fd2, "DecimalValue")
		mdA, _ := GetMessageDescriptorByName(fd, "MergeA")

		merged, err := MergeMessageDescriptors(mdA, mdDecimal, "RenumberGapTest")
		if err != nil {
			t.Fatalf("MergeMessageDescriptors() error = %v", err)
		}

		fields := merged.Fields()
		// B (DecimalValue) fields should start at 3 (max of A is 2)
		unitsField := fields.ByName("units")
		if unitsField == nil {
			t.Fatal("expected field 'units'")
		}
		if unitsField.Number() != 3 {
			t.Errorf("expected units field number 3, got %d", unitsField.Number())
		}

		nanosField := fields.ByName("nanos")
		if nanosField == nil {
			t.Fatal("expected field 'nanos'")
		}
		if nanosField.Number() != 4 {
			t.Errorf("expected nanos field number 4, got %d", nanosField.Number())
		}
	})
}

func TestMergeMessageDescriptors_NestedTypes(t *testing.T) {
	protoDir := testProtoDir(t)

	fd, err := CompileProtoToFileDescriptor("merge_nested_test.proto", []string{protoDir})
	if err != nil {
		t.Fatalf("setup: CompileProtoToFileDescriptor() error = %v", err)
	}

	t.Run("merge_with_nested_messages", func(t *testing.T) {
		// WithNestedA has InnerA, WithNestedB has InnerB — no conflict
		mdA, err := GetMessageDescriptorByName(fd, "WithNestedA")
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		mdB, err := GetMessageDescriptorByName(fd, "WithNestedB")
		if err != nil {
			t.Fatalf("setup: %v", err)
		}

		merged, err := MergeMessageDescriptors(mdA, mdB, "MergedNested")
		if err != nil {
			t.Fatalf("MergeMessageDescriptors() error = %v", err)
		}

		// Check fields from both messages are present
		fields := merged.Fields()
		fieldNames := make(map[string]bool)
		for i := 0; i < fields.Len(); i++ {
			fieldNames[string(fields.Get(i).Name())] = true
		}
		for _, want := range []string{"label", "detail", "count", "info"} {
			if !fieldNames[want] {
				t.Errorf("expected field %q in merged descriptor, have %v", want, fieldNames)
			}
		}
	})

	t.Run("nested_message_conflict", func(t *testing.T) {
		// WithNestedA has InnerA, WithNestedConflict also has InnerA
		mdA, err := GetMessageDescriptorByName(fd, "WithNestedA")
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		mdConflict, err := GetMessageDescriptorByName(fd, "WithNestedConflict")
		if err != nil {
			t.Fatalf("setup: %v", err)
		}

		_, err = MergeMessageDescriptors(mdA, mdConflict, "NestedConflict")
		if err == nil {
			t.Fatal("expected nested message type name conflict error")
		}
		if got := err.Error(); !contains(got, "nested message type name conflict") {
			t.Errorf("expected 'nested message type name conflict' error, got: %v", err)
		}
	})
}

func TestMergeMessageDescriptors_Enums(t *testing.T) {
	protoDir := testProtoDir(t)

	fd, err := CompileProtoToFileDescriptor("merge_nested_test.proto", []string{protoDir})
	if err != nil {
		t.Fatalf("setup: CompileProtoToFileDescriptor() error = %v", err)
	}

	t.Run("merge_with_enums", func(t *testing.T) {
		// WithEnumA has StatusA, WithEnumB has LevelB — no conflict
		mdA, err := GetMessageDescriptorByName(fd, "WithEnumA")
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		mdB, err := GetMessageDescriptorByName(fd, "WithEnumB")
		if err != nil {
			t.Fatalf("setup: %v", err)
		}

		merged, err := MergeMessageDescriptors(mdA, mdB, "MergedEnums")
		if err != nil {
			t.Fatalf("MergeMessageDescriptors() error = %v", err)
		}

		// Check fields from both
		fields := merged.Fields()
		fieldNames := make(map[string]bool)
		for i := 0; i < fields.Len(); i++ {
			fieldNames[string(fields.Get(i).Name())] = true
		}
		for _, want := range []string{"name", "status", "priority", "level"} {
			if !fieldNames[want] {
				t.Errorf("expected field %q in merged descriptor, have %v", want, fieldNames)
			}
		}
	})

	t.Run("enum_conflict", func(t *testing.T) {
		// WithEnumA has StatusA, WithEnumConflict also has StatusA
		mdA, err := GetMessageDescriptorByName(fd, "WithEnumA")
		if err != nil {
			t.Fatalf("setup: %v", err)
		}
		mdConflict, err := GetMessageDescriptorByName(fd, "WithEnumConflict")
		if err != nil {
			t.Fatalf("setup: %v", err)
		}

		_, err = MergeMessageDescriptors(mdA, mdConflict, "EnumConflict")
		if err == nil {
			t.Fatal("expected enum type name conflict error")
		}
		if got := err.Error(); !contains(got, "enum type name conflict") {
			t.Errorf("expected 'enum type name conflict' error, got: %v", err)
		}
	})
}

// contains is a test helper for substring matching.
func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchSubstring(s, substr)
}

func searchSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// compilePruneProto compiles prune_test.proto from the samples directory and
// returns both the FileDescriptor and a lookup helper for message descriptors.
func compilePruneProto(t *testing.T) (lookup func(name string) protoreflect.MessageDescriptor) {
	t.Helper()
	protoDir := testProtoDir(t)
	fd, err := CompileProtoToFileDescriptor("prune_test.proto", []string{protoDir})
	if err != nil {
		t.Fatalf("setup: CompileProtoToFileDescriptor(prune_test.proto) error = %v", err)
	}
	return func(name string) protoreflect.MessageDescriptor {
		md, err := GetMessageDescriptorByName(fd, name)
		if err != nil {
			t.Fatalf("setup: GetMessageDescriptorByName(%q) error = %v", name, err)
		}
		return md
	}
}

func TestPruneEmptyMessages(t *testing.T) {
	get := compilePruneProto(t)

	t.Run("scalar_only_unchanged", func(t *testing.T) {
		// ScalarOnly has no message-type fields; PruneEmptyMessages should return
		// an equivalent descriptor with the same fields.
		md := get("ScalarOnly")
		pruned, err := PruneEmptyMessages(md)
		if err != nil {
			t.Fatalf("PruneEmptyMessages() error = %v", err)
		}
		if pruned.Fields().Len() != md.Fields().Len() {
			t.Errorf("expected %d fields, got %d", md.Fields().Len(), pruned.Fields().Len())
		}
		for i := 0; i < md.Fields().Len(); i++ {
			want := string(md.Fields().Get(i).Name())
			if pruned.Fields().ByName(protoreflect.Name(want)) == nil {
				t.Errorf("expected field %q to survive pruning", want)
			}
		}
	})

	t.Run("empty_field_removed", func(t *testing.T) {
		// HasEmptyField has empty_msg(1) of type EmptyMsg and name(2) string.
		// After pruning, only name should remain.
		md := get("HasEmptyField")
		pruned, err := PruneEmptyMessages(md)
		if err != nil {
			t.Fatalf("PruneEmptyMessages() error = %v", err)
		}
		if pruned.Fields().Len() != 1 {
			t.Errorf("expected 1 field, got %d", pruned.Fields().Len())
		}
		if pruned.Fields().ByName("name") == nil {
			t.Error("expected field 'name' to survive pruning")
		}
		if pruned.Fields().ByName("empty_msg") != nil {
			t.Error("expected field 'empty_msg' to be pruned")
		}
	})

	t.Run("field_numbers_unchanged", func(t *testing.T) {
		// Field numbers of surviving fields must not change.
		md := get("HasEmptyField")
		pruned, err := PruneEmptyMessages(md)
		if err != nil {
			t.Fatalf("PruneEmptyMessages() error = %v", err)
		}
		nameField := pruned.Fields().ByName("name")
		if nameField == nil {
			t.Fatal("expected field 'name'")
		}
		if nameField.Number() != 2 {
			t.Errorf("expected 'name' to retain field number 2, got %d", nameField.Number())
		}
	})

	t.Run("all_empty_fields_returns_error", func(t *testing.T) {
		// AllEmptyFields has only fields of type EmptyMsg; pruning should error.
		md := get("AllEmptyFields")
		_, err := PruneEmptyMessages(md)
		if err == nil {
			t.Fatal("expected error when all fields are pruned, got nil")
		}
	})

	t.Run("cascade_prune", func(t *testing.T) {
		// CascadePrune has inner_empty(1) of type InnerEmpty and name(2) string.
		// InnerEmpty itself has only an EmptyMsg field; after that field is pruned,
		// InnerEmpty becomes effectively empty, so inner_empty is also removed.
		// Only name should survive.
		md := get("CascadePrune")
		pruned, err := PruneEmptyMessages(md)
		if err != nil {
			t.Fatalf("PruneEmptyMessages() error = %v", err)
		}
		if pruned.Fields().Len() != 1 {
			t.Errorf("expected 1 field after cascade prune, got %d", pruned.Fields().Len())
		}
		if pruned.Fields().ByName("name") == nil {
			t.Error("expected field 'name' to survive cascade prune")
		}
		if pruned.Fields().ByName("inner_empty") != nil {
			t.Error("expected field 'inner_empty' to be cascade-pruned")
		}
	})

	t.Run("oneof_mixed_variants", func(t *testing.T) {
		// OneofMixed has oneof value { string s = 1; EmptyMsg e = 2; }.
		// After pruning, the scalar variant s should survive and the oneof
		// declaration should still exist.
		md := get("OneofMixed")
		pruned, err := PruneEmptyMessages(md)
		if err != nil {
			t.Fatalf("PruneEmptyMessages() error = %v", err)
		}
		if pruned.Fields().Len() != 1 {
			t.Errorf("expected 1 surviving field, got %d", pruned.Fields().Len())
		}
		sField := pruned.Fields().ByName("s")
		if sField == nil {
			t.Fatal("expected field 's' to survive")
		}
		if sField.ContainingOneof() == nil {
			t.Error("expected 's' to still belong to a oneof after pruning")
		}
	})

	t.Run("oneof_all_empty_decl_removed", func(t *testing.T) {
		// OneofAllEmpty has name(1) and oneof ghost { EmptyMsg e = 2; }.
		// After pruning, the ghost oneof should be gone entirely; name survives.
		md := get("OneofAllEmpty")
		pruned, err := PruneEmptyMessages(md)
		if err != nil {
			t.Fatalf("PruneEmptyMessages() error = %v", err)
		}
		if pruned.Fields().Len() != 1 {
			t.Errorf("expected 1 surviving field, got %d", pruned.Fields().Len())
		}
		if pruned.Fields().ByName("name") == nil {
			t.Error("expected field 'name' to survive")
		}
		if pruned.Oneofs().Len() != 0 {
			t.Errorf("expected 0 oneof declarations after removing orphaned oneof, got %d", pruned.Oneofs().Len())
		}
	})

	t.Run("map_field_preserved", func(t *testing.T) {
		// MapPreserved has map<string, ScalarOnly> items = 1.
		// Map fields must never be pruned regardless of value type.
		md := get("MapPreserved")
		pruned, err := PruneEmptyMessages(md)
		if err != nil {
			t.Fatalf("PruneEmptyMessages() error = %v", err)
		}
		if pruned.Fields().Len() != 1 {
			t.Errorf("expected 1 map field to survive, got %d", pruned.Fields().Len())
		}
		itemsField := pruned.Fields().ByName("items")
		if itemsField == nil {
			t.Error("expected map field 'items' to survive")
		}
		if !itemsField.IsMap() {
			t.Error("expected 'items' to still be a map field after pruning")
		}
	})

	t.Run("pruned_descriptor_is_usable", func(t *testing.T) {
		// Sanity: the pruned descriptor can be used to look up surviving fields
			// and the parent file preserves the source package and has the expected
			// synthetic path.
		md := get("HasEmptyField")
		pruned, err := PruneEmptyMessages(md)
		if err != nil {
			t.Fatalf("PruneEmptyMessages() error = %v", err)
		}
			if pruned.ParentFile().Package() != md.ParentFile().Package() {
				t.Errorf("expected package %q, got %q", md.ParentFile().Package(), pruned.ParentFile().Package())
			}
		parentPath := string(pruned.ParentFile().Path())
		if parentPath != "prune_test_pruned.proto" {
			t.Errorf("expected synthetic parent path prune_test_pruned.proto, got %s", parentPath)
		}
		if pruned.ParentFile().Syntax() != protoreflect.Proto3 {
			t.Error("expected proto3 syntax on pruned descriptor")
		}
	})

	t.Run("prunes_fields_inside_referenced_top_level_message", func(t *testing.T) {
		// RootTopLevelRef -> ParentTopLevel. ParentTopLevel has an EmptyMsg field
		// plus a scalar. Pruning must affect ParentTopLevel's definition too.
		md := get("RootTopLevelRef")
		pruned, err := PruneEmptyMessages(md)
		if err != nil {
			t.Fatalf("PruneEmptyMessages() error = %v", err)
		}

		parentField := pruned.Fields().ByName("parent")
		if parentField == nil {
			t.Fatal("expected field 'parent' to survive")
		}
		parentMD := parentField.Message()
		if parentMD == nil {
			t.Fatal("expected 'parent' to be a message field")
		}
		if parentMD.Fields().ByName("empty") != nil {
			t.Error("expected ParentTopLevel.empty to be pruned")
		}
		if parentMD.Fields().ByName("kept") == nil {
			t.Error("expected ParentTopLevel.kept to survive")
		}
	})

}
