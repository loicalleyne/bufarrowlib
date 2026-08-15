# Grounding artifact: protojson against recursive well-known types

Produced 2026-08-15 for `plan/RecursiveWkt.prompt.md`. Records empirically
observed behaviour that the plan depends on, rather than assumed behaviour.

Pinned versions (`go.mod`):

- `google.golang.org/protobuf v1.36.11`
- `buf.build/go/hyperpb v0.1.3`

Probe: a temporary `TestScratchGrounding` compiling `testdata/proto/wkt_test.proto`
via `CompileProtoToFileDescriptor` (protocompile), exercising both the
`dynamicpb` and `hyperpb` reflection backends.

## Findings

| # | Question | Result |
|---|---|---|
| Q1 | `protojson.Marshal` on an **unset** `google.protobuf.Value` | **Fails**: `proto: google.protobuf.Value: none of the oneof fields is set` |
| Q1 | `protoreflect.Value` shape for an unset `Value` field | outer `IsValid()` = **true**, `Message().IsValid()` = **false**, `Has()` = false |
| Q2 | `protojson.Marshal` on an **unset** `google.protobuf.Struct` | Succeeds, yields `{}` |
| Q3 | `hyperpb.CompileMessageDescriptor` on a descriptor containing `google.protobuf.Value` | Compiles, no panic |
| Q4 | `protojson.Marshal` on a **hyperpb-backed** `google.protobuf.Value` | Succeeds, yields `"hello"` |
| Q5 | hyperpb unset `Value` field | `Message().IsValid()` = **false**, same as dynamicpb |

## Consequences for the implementation

1. **The unset guard is required and must use `Message().IsValid()`.** The outer
   `protoreflect.Value.IsValid()` returns true for an unset message field, so it
   is not a usable predicate on its own. `Message().IsValid()` returns false on
   both backends.
2. **The marshal-error branch is also required.** A *present but empty* `Value`
   (for example a map entry explicitly set to `Value{}`) passes the `IsValid()`
   guard and still fails to marshal. Both guards are needed; neither alone is
   sufficient.
3. `Struct` and `ListValue` do not fail on empty input, so the guards are
   defensive for those two and load-bearing only for `Value`.
4. **protojson works on hyperpb**, so the `AppendRaw` path needs no special
   casing. The value is returned as a Go `[]byte` from `protojson.Marshal` and
   copied into the Arrow builder by `Append`, so no reference into the hyperpb
   arena survives `Shared.Free()`.
