package bufarrowlib

import "github.com/apache/arrow-go/v18/arrow"

// MetaCol identifies a metadata column declared with
// WithDenormMetadataColumns.
type MetaCol string

// DenormMetadataColumn declares a metadata column appended to the denormalized
// schema after plan-derived columns.
type DenormMetadataColumn struct {
	Name MetaCol
	Type arrow.DataType
}

// DenormMetaValue is a name-keyed metadata value attached to an AppendDenorm*
// call.
type DenormMetaValue struct {
	Col   MetaCol
	Value any
}

// Meta builds a DenormMetaValue.
//
// Pass an untyped nil (or omit the column) to represent "no value" for a
// message. A typed nil pointer (for example, (*int32)(nil)) is a non-nil any
// and fails type validation for the metadata column.
func Meta(col MetaCol, v any) DenormMetaValue {
	return DenormMetaValue{Col: col, Value: v}
}

// DenormMetaColumns returns registered metadata column declarations in the same
// order used by the denormalizer schema and append path.
func (s *Transcoder) DenormMetaColumns() []DenormMetadataColumn {
	if len(s.denormMetaCols) == 0 {
		return nil
	}
	out := make([]DenormMetadataColumn, len(s.denormMetaCols))
	for i, col := range s.denormMetaCols {
		out[i] = DenormMetadataColumn{Name: col.col, Type: col.typ}
	}
	return out
}
