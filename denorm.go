package bufarrowlib

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/loicalleyne/bufarrowlib/proto/pbpath"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// fanoutGroup represents a set of columns that share the same fan-out
// signature — i.e. the same sequence of ListWildcardStep and ListRangeStep
// positions along their paths. Columns within a group always produce the same
// number of branches for a given message, so they advance in lockstep during
// row iteration. Different groups are cross-joined.
type fanoutGroup struct {
	// sig is the fan-out signature string (e.g. "W2.R5") used as the grouping key.
	sig string
	// colIndices are the column indices (into Transcoder.denormCols) that belong
	// to this group.
	colIndices []int
}

// denormColumn holds the per-column metadata and append closures compiled once
// at plan compilation time and reused on every AppendDenorm call.
type denormColumn struct {
	// entryIdx is the index into Plan.Entries() / Eval() results for this column.
	entryIdx int
	// groupIdx is the index into Transcoder.denormGroups for this column's fan-out group.
	groupIdx int
	// appendFn appends a scalar protoreflect.Value to the column's Arrow builder.
	appendFn protoAppendFunc
	// fd is the leaf field descriptor (used for type information).
	fd protoreflect.FieldDescriptor
}

// fanoutSignature computes a string key that identifies the fan-out shape of a
// compiled path. Only ListWildcardStep and ListRangeStep contribute to the
// signature; ListIndexStep is excluded because it selects a single element
// (scalar broadcast) rather than producing fan-out.
//
// The signature encodes both the step position and the preceding field name
// so that wildcards on different repeated fields at the same depth produce
// distinct signatures.
//
// Two paths with the same signature produce the same number of branches for any
// given message and therefore belong to the same fan-out group.
func fanoutSignature(p pbpath.Path) string {
	var b strings.Builder
	for i, step := range p {
		switch step.Kind() {
		case pbpath.ListWildcardStep:
			if b.Len() > 0 {
				b.WriteByte('.')
			}
			// Include the parent field name for disambiguation.
			parentField := ""
			if i > 0 && p[i-1].Kind() == pbpath.FieldAccessStep {
				parentField = string(p[i-1].FieldDescriptor().Name())
			}
			fmt.Fprintf(&b, "W%d:%s", i, parentField)
		case pbpath.ListRangeStep:
			if b.Len() > 0 {
				b.WriteByte('.')
			}
			parentField := ""
			if i > 0 && p[i-1].Kind() == pbpath.FieldAccessStep {
				parentField = string(p[i-1].FieldDescriptor().Name())
			}
			fmt.Fprintf(&b, "R%d:%s[%d:%d:%d]", i, parentField, step.RangeStart(), step.RangeEnd(), step.RangeStep())
		}
	}
	return b.String()
}

// compileDenormPlan compiles the denormalizer plan from the PlanPathSpec
// entries stored in opts.denormPaths. It validates that every leaf is a
// recognised scalar type, computes fan-out groups, builds the Arrow schema
// and record builder, and wires per-column append closures.
//
// Called from New() and Clone().
func (s *Transcoder) compileDenormPlan(mem memory.Allocator) error {
	plan, err := pbpath.NewPlan(s.msgDesc, s.opts.denormPaths...)
	if err != nil {
		return fmt.Errorf("bufarrow: denormalizer plan: %w", err)
	}
	s.denormPlan = plan

	entries := plan.Entries()
	nCols := len(entries)

	// --- Validate leaves & build Arrow schema fields ---
	arrowFields := make([]arrow.Field, nCols)
	leafFDs := make([]protoreflect.FieldDescriptor, nCols)

	for i, e := range entries {
		// Find the leaf FieldDescriptor. For paths ending in a FieldAccessStep
		// this is the last step directly. For paths ending in a list step
		// (wildcard, range, index) applied to a repeated scalar, the field
		// descriptor is on the preceding FieldAccessStep.
		var fd protoreflect.FieldDescriptor
		for j := len(e.Path) - 1; j >= 0; j-- {
			if e.Path[j].Kind() == pbpath.FieldAccessStep {
				fd = e.Path[j].FieldDescriptor()
				break
			}
		}
		if fd == nil {
			return fmt.Errorf("bufarrow: denormalizer path %q does not contain a field access step", e.Name)
		}
		at := ProtoKindToArrowType(fd)
		if at == nil {
			return fmt.Errorf("bufarrow: denormalizer path %q terminates at unsupported type %v (message-typed leaves must use DenormalizerBuilder for custom logic)", e.Name, fd.Kind())
		}
		leafFDs[i] = fd
		arrowFields[i] = arrow.Field{
			Name:     e.Name,
			Type:     at,
			Nullable: true, // all denorm columns are nullable (left-join may produce nulls)
		}
	}

	s.denormSchema = arrow.NewSchema(arrowFields, nil)
	s.denormBuilder = array.NewRecordBuilder(mem, s.denormSchema)

	// --- Compute fan-out groups ---
	sigToGroup := make(map[string]int)      // signature → group index
	s.denormGroups = s.denormGroups[:0]      // reset for Clone()
	s.denormCols = make([]denormColumn, nCols)

	for i, e := range entries {
		sig := fanoutSignature(e.Path)
		gIdx, ok := sigToGroup[sig]
		if !ok {
			gIdx = len(s.denormGroups)
			sigToGroup[sig] = gIdx
			s.denormGroups = append(s.denormGroups, fanoutGroup{sig: sig})
		}
		s.denormGroups[gIdx].colIndices = append(s.denormGroups[gIdx].colIndices, i)

		fn := ProtoKindToAppendFunc(leafFDs[i], s.denormBuilder.Field(i))
		if fn == nil {
			return fmt.Errorf("bufarrow: failed to create append function for denorm path %q", e.Name)
		}
		s.denormCols[i] = denormColumn{
			entryIdx: i,
			groupIdx: gIdx,
			appendFn: fn,
			fd:       leafFDs[i],
		}
	}

	return nil
}

// AppendDenorm evaluates the denormalizer plan against msg and appends the
// resulting denormalized rows to the denormalizer's Arrow record builder.
//
// Fan-out groups are cross-joined: each group contributes max(len(branches), 1)
// rows, and the total row count is the product of all group counts. Empty
// fan-out groups (no branches) contribute 1 null row (left-join semantics).
//
// This method is not safe for concurrent use.
func (s *Transcoder) AppendDenorm(msg proto.Message) error {
	if s.denormPlan == nil {
		return fmt.Errorf("bufarrow: AppendDenorm called without denormalizer plan configured")
	}

	results, err := s.denormPlan.Eval(msg)
	if err != nil {
		return fmt.Errorf("bufarrow: denormalizer eval: %w", err)
	}

	nGroups := len(s.denormGroups)

	// --- Compute per-group row counts ---
	groupCounts := make([]int, nGroups)
	groupIsNull := make([]bool, nGroups) // true if this group left-joins to null
	for g := range s.denormGroups {
		// All columns in a group have the same branch count; pick the first.
		firstCol := s.denormGroups[g].colIndices[0]
		branches := results[firstCol]
		if len(branches) == 0 {
			groupCounts[g] = 1
			groupIsNull[g] = true
		} else {
			groupCounts[g] = len(branches)
		}
	}

	// Total rows = product of all group counts.
	totalRows := 1
	for _, c := range groupCounts {
		totalRows *= c
	}
	if totalRows == 0 {
		return nil
	}

	// --- Null fast-path: bulk-append nulls for entirely-null groups ---
	// For groups where groupIsNull is true, every column in that group gets
	// AppendNulls(totalRows) and is excluded from the per-row loop.
	nullGroupCols := make(map[int]bool) // column indices to skip in per-row loop
	for g := range s.denormGroups {
		if groupIsNull[g] {
			for _, colIdx := range s.denormGroups[g].colIndices {
				s.denormBuilder.Field(colIdx).AppendNulls(totalRows)
				nullGroupCols[colIdx] = true
			}
		}
	}

	// If every group is null, we're done — all columns already got their nulls.
	if len(nullGroupCols) == len(s.denormCols) {
		return nil
	}

	// --- Per-row iteration with div/mod cross-join ---
	for row := 0; row < totalRows; row++ {
		remainder := row
		// Walk groups from last to first to compute the branch index for each.
		branchIdx := make([]int, nGroups)
		for g := nGroups - 1; g >= 0; g-- {
			branchIdx[g] = remainder % groupCounts[g]
			remainder /= groupCounts[g]
		}

		// Append values for each non-null column.
		for colIdx, col := range s.denormCols {
			if nullGroupCols[colIdx] {
				continue // already bulk-appended
			}
			branches := results[col.entryIdx]
			bIdx := branchIdx[col.groupIdx]
			if bIdx >= len(branches) {
				// Shouldn't happen if group counts are correct, but be safe.
				s.denormBuilder.Field(colIdx).AppendNull()
				continue
			}
			leaf := branches[bIdx].Index(-1)
			if !leaf.Value.IsValid() {
				s.denormBuilder.Field(colIdx).AppendNull()
				continue
			}
			col.appendFn(leaf.Value)
		}
	}

	return nil
}
