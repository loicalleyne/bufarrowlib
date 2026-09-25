package bufarrowlib

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// TestWKTFlattenEncodeGoogleTypeDateAndTimeOfDay verifies decode paths for
// google.type.Date and google.type.TimeOfDay flattened Arrow columns.
func TestWKTFlattenEncodeGoogleTypeDateAndTimeOfDay(t *testing.T) {
	md := buildWKTFiles(t)
	alloc := memory.DefaultAllocator

	t.Run("date", func(t *testing.T) {
		fd := md.Fields().ByName("date")
		enc := wktFlattenEncode(fd)
		if enc == nil {
			t.Fatal("wktFlattenEncode(date) returned nil")
		}

		dateBuilder := array.NewDate32Builder(alloc)
		defer dateBuilder.Release()
		dateBuilder.Append(arrow.Date32FromTime(time.Date(2026, time.May, 22, 0, 0, 0, 0, time.UTC)))
		arr := dateBuilder.NewDate32Array()
		defer arr.Release()

		msg := dynamicpb.NewMessage(fd.Message())
		got := enc(protoreflect.ValueOfMessage(msg), arr, 0)
		if !got.IsValid() {
			t.Fatal("expected valid decoded date value")
		}
		m := got.Message()
		if m.Get(m.Descriptor().Fields().ByName("year")).Int() != 2026 {
			t.Fatalf("year mismatch: got %d", m.Get(m.Descriptor().Fields().ByName("year")).Int())
		}
		if m.Get(m.Descriptor().Fields().ByName("month")).Int() != 5 {
			t.Fatalf("month mismatch: got %d", m.Get(m.Descriptor().Fields().ByName("month")).Int())
		}
		if m.Get(m.Descriptor().Fields().ByName("day")).Int() != 22 {
			t.Fatalf("day mismatch: got %d", m.Get(m.Descriptor().Fields().ByName("day")).Int())
		}
	})

	t.Run("timeofday", func(t *testing.T) {
		fd := md.Fields().ByName("tod")
		enc := wktFlattenEncode(fd)
		if enc == nil {
			t.Fatal("wktFlattenEncode(tod) returned nil")
		}

		timeBuilder := array.NewTime64Builder(alloc, &arrow.Time64Type{Unit: arrow.Microsecond})
		defer timeBuilder.Release()
		us := int64((2*time.Hour + 3*time.Minute + 4*time.Second + 5*time.Millisecond) / time.Microsecond)
		timeBuilder.Append(arrow.Time64(us))
		arr := timeBuilder.NewTime64Array()
		defer arr.Release()

		msg := dynamicpb.NewMessage(fd.Message())
		got := enc(protoreflect.ValueOfMessage(msg), arr, 0)
		if !got.IsValid() {
			t.Fatal("expected valid decoded timeofday value")
		}
		m := got.Message()
		if m.Get(m.Descriptor().Fields().ByName("hours")).Int() != 2 {
			t.Fatalf("hours mismatch: got %d", m.Get(m.Descriptor().Fields().ByName("hours")).Int())
		}
		if m.Get(m.Descriptor().Fields().ByName("minutes")).Int() != 3 {
			t.Fatalf("minutes mismatch: got %d", m.Get(m.Descriptor().Fields().ByName("minutes")).Int())
		}
		if m.Get(m.Descriptor().Fields().ByName("seconds")).Int() != 4 {
			t.Fatalf("seconds mismatch: got %d", m.Get(m.Descriptor().Fields().ByName("seconds")).Int())
		}
		if m.Get(m.Descriptor().Fields().ByName("nanos")).Int() != 5_000_000 {
			t.Fatalf("nanos mismatch: got %d", m.Get(m.Descriptor().Fields().ByName("nanos")).Int())
		}
	})
}

// TestWKTFlattenEncodeProtoJSONMessages verifies JSON-backed decode branches
// (Money/LatLng/Color/PostalAddress/Interval) succeed on valid JSON and return
// zero values on malformed JSON.
func TestWKTFlattenEncodeProtoJSONMessages(t *testing.T) {
	md := buildWKTFiles(t)
	alloc := memory.DefaultAllocator

	tests := []struct {
		field string
		json  string
		probe func(m protoreflect.Message) bool
	}{
		{
			field: "money",
			json:  `{"currencyCode":"USD","units":"12","nanos":500000000}`,
			probe: func(m protoreflect.Message) bool {
				return m.Get(m.Descriptor().Fields().ByName("currency_code")).String() == "USD"
			},
		},
		{
			field: "latlng",
			json:  `{"latitude":45.5,"longitude":-73.56}`,
			probe: func(m protoreflect.Message) bool {
				return m.Get(m.Descriptor().Fields().ByName("latitude")).Float() != 0
			},
		},
		{
			field: "color",
			json:  `{"red":0.1,"green":0.2,"blue":0.3}`,
			probe: func(m protoreflect.Message) bool {
				return m.Get(m.Descriptor().Fields().ByName("red")).Float() != 0
			},
		},
		{
			field: "address",
			json:  `{"regionCode":"CA","postalCode":"H2X1Y4"}`,
			probe: func(m protoreflect.Message) bool {
				return m.Get(m.Descriptor().Fields().ByName("region_code")).String() == "CA"
			},
		},
		{
			field: "interval",
			json:  `{"startTime":"2026-05-22T10:00:00Z","endTime":"2026-05-22T12:00:00Z"}`,
			probe: func(m protoreflect.Message) bool {
				return m.Has(m.Descriptor().Fields().ByName("start_time"))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.field+"-valid", func(t *testing.T) {
			fd := md.Fields().ByName(protoreflect.Name(tc.field))
			enc := wktFlattenEncode(fd)
			if enc == nil {
				t.Fatalf("wktFlattenEncode(%s) returned nil", tc.field)
			}

			sb := array.NewStringBuilder(alloc)
			defer sb.Release()
			sb.Append(tc.json)
			arr := sb.NewStringArray()
			defer arr.Release()

			msg := dynamicpb.NewMessage(fd.Message())
			got := enc(protoreflect.ValueOfMessage(msg), arr, 0)
			if !got.IsValid() {
				t.Fatalf("expected valid decoded %s value", tc.field)
			}
			if !tc.probe(got.Message()) {
				t.Fatalf("decoded %s value did not contain expected fields", tc.field)
			}
		})

		t.Run(tc.field+"-invalid", func(t *testing.T) {
			fd := md.Fields().ByName(protoreflect.Name(tc.field))
			enc := wktFlattenEncode(fd)
			sb := array.NewStringBuilder(alloc)
			defer sb.Release()
			sb.Append("{not-json")
			arr := sb.NewStringArray()
			defer arr.Release()

			msg := dynamicpb.NewMessage(fd.Message())
			got := enc(protoreflect.ValueOfMessage(msg), arr, 0)
			if got.IsValid() {
				t.Fatalf("expected invalid value for malformed %s json", tc.field)
			}
		})
	}
}

// TestWKTFlattenEncodeDurationAndEmptyFieldMask covers decode branches that
// are not always reached through end-to-end schema round-trips.
func TestWKTFlattenEncodeDurationAndEmptyFieldMask(t *testing.T) {
	md := buildWKTFiles(t)
	alloc := memory.DefaultAllocator

	t.Run("duration", func(t *testing.T) {
		fd := md.Fields().ByName("dur")
		enc := wktFlattenEncode(fd)
		if enc == nil {
			t.Fatal("wktFlattenEncode(dur) returned nil")
		}

		db := array.NewDurationBuilder(alloc, &arrow.DurationType{Unit: arrow.Millisecond})
		defer db.Release()
		db.Append(arrow.Duration(-1500))
		arr := db.NewDurationArray()
		defer arr.Release()

		msg := dynamicpb.NewMessage(fd.Message())
		got := enc(protoreflect.ValueOfMessage(msg), arr, 0)
		if !got.IsValid() {
			t.Fatal("expected valid decoded duration value")
		}
		m := got.Message()
		if sec := m.Get(m.Descriptor().Fields().ByName("seconds")).Int(); sec != -1 {
			t.Fatalf("duration seconds = %d, want -1", sec)
		}
		if nanos := m.Get(m.Descriptor().Fields().ByName("nanos")).Int(); nanos != -500000000 {
			t.Fatalf("duration nanos = %d, want -500000000", nanos)
		}
	})

	t.Run("empty fieldmask", func(t *testing.T) {
		fd := md.Fields().ByName("mask")
		enc := wktFlattenEncode(fd)
		if enc == nil {
			t.Fatal("wktFlattenEncode(mask) returned nil")
		}

		sb := array.NewStringBuilder(alloc)
		defer sb.Release()
		sb.Append("")
		arr := sb.NewStringArray()
		defer arr.Release()

		msg := dynamicpb.NewMessage(fd.Message())
		got := enc(protoreflect.ValueOfMessage(msg), arr, 0)
		if !got.IsValid() {
			t.Fatal("expected valid decoded fieldmask value")
		}
		pathsFD := got.Message().Descriptor().Fields().ByName("paths")
		if got.Message().Get(pathsFD).List().Len() != 0 {
			t.Fatalf("fieldmask path count = %d, want 0", got.Message().Get(pathsFD).List().Len())
		}
	})
}
