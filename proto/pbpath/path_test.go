package pbpath

import (
	"testing"

	"github.com/sryoya/protorand"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// ---------- testmessage.proto descriptor + generator ----------

// buildTestMessageDescriptor constructs the descriptor for testmessage.proto:
//
//	package pbpath.testdata;
//	message Test {
//	  message Nested {
//	    int32  intfield     = 1;
//	    string stringfield  = 2;
//	    bytes  bytesfield   = 3;
//	    Test   nested       = 4;
//	  }
//	  Nested              nested        = 1;
//	  repeated Test       repeats       = 2;
//	  repeated int32      int32repeats  = 3;
//	  map<string, Nested> strkeymap     = 4;
//	  map<bool,   Test>   boolkeymap    = 5;
//	  map<int32,  Test>   int32keymap   = 6;
//	  map<int64,  Test>   int64keymap   = 7;
//	  map<uint32, Test>   uint32keymap  = 8;
//	  map<uint64, Test>   uint64keymap  = 9;
//	}
func buildTestMessageDescriptor(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()

	stringType := descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
	messageType := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
	int32Type := descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum()
	bytesType := descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum()
	boolType := descriptorpb.FieldDescriptorProto_TYPE_BOOL.Enum()
	uint64Type := descriptorpb.FieldDescriptorProto_TYPE_UINT64.Enum()
	uint32Type := descriptorpb.FieldDescriptorProto_TYPE_UINT32.Enum()
	int64Type := descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum()
	labelOptional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
	labelRepeated := descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()

	mapEntry := func(name string, keyType *descriptorpb.FieldDescriptorProto_Type, valueTypeName string) *descriptorpb.DescriptorProto {
		return &descriptorpb.DescriptorProto{
			Name: proto.String(name),
			Field: []*descriptorpb.FieldDescriptorProto{
				{Name: proto.String("key"), Number: proto.Int32(1), Type: keyType, Label: labelOptional},
				{Name: proto.String("value"), Number: proto.Int32(2), Type: messageType, TypeName: proto.String(valueTypeName), Label: labelOptional},
			},
			Options: &descriptorpb.MessageOptions{MapEntry: proto.Bool(true)},
		}
	}

	pkg := "pbpath.testdata"
	fdp := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("testmessage.proto"),
		Package: proto.String(pkg),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("Test"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{Name: proto.String("nested"), Number: proto.Int32(1), Type: messageType, TypeName: proto.String("." + pkg + ".Test.Nested"), Label: labelOptional},
					{Name: proto.String("repeats"), Number: proto.Int32(2), Type: messageType, TypeName: proto.String("." + pkg + ".Test"), Label: labelRepeated},
					{Name: proto.String("int32repeats"), Number: proto.Int32(3), Type: int32Type, Label: labelRepeated},
					{Name: proto.String("strkeymap"), Number: proto.Int32(4), Type: messageType, TypeName: proto.String("." + pkg + ".Test.StrkeymapEntry"), Label: labelRepeated},
					{Name: proto.String("boolkeymap"), Number: proto.Int32(5), Type: messageType, TypeName: proto.String("." + pkg + ".Test.BoolkeymapEntry"), Label: labelRepeated},
					{Name: proto.String("int32keymap"), Number: proto.Int32(6), Type: messageType, TypeName: proto.String("." + pkg + ".Test.Int32keymapEntry"), Label: labelRepeated},
					{Name: proto.String("int64keymap"), Number: proto.Int32(7), Type: messageType, TypeName: proto.String("." + pkg + ".Test.Int64keymapEntry"), Label: labelRepeated},
					{Name: proto.String("uint32keymap"), Number: proto.Int32(8), Type: messageType, TypeName: proto.String("." + pkg + ".Test.Uint32keymapEntry"), Label: labelRepeated},
					{Name: proto.String("uint64keymap"), Number: proto.Int32(9), Type: messageType, TypeName: proto.String("." + pkg + ".Test.Uint64keymapEntry"), Label: labelRepeated},
				},
				NestedType: []*descriptorpb.DescriptorProto{
					{
						Name: proto.String("Nested"),
						Field: []*descriptorpb.FieldDescriptorProto{
							{Name: proto.String("intfield"), Number: proto.Int32(1), Type: int32Type, Label: labelOptional},
							{Name: proto.String("stringfield"), Number: proto.Int32(2), Type: stringType, Label: labelOptional},
							{Name: proto.String("bytesfield"), Number: proto.Int32(3), Type: bytesType, Label: labelOptional},
							{Name: proto.String("nested"), Number: proto.Int32(4), Type: messageType, TypeName: proto.String("." + pkg + ".Test"), Label: labelOptional},
						},
					},
					mapEntry("StrkeymapEntry", stringType, "."+pkg+".Test.Nested"),
					mapEntry("BoolkeymapEntry", boolType, "."+pkg+".Test"),
					mapEntry("Int32keymapEntry", int32Type, "."+pkg+".Test"),
					mapEntry("Int64keymapEntry", int64Type, "."+pkg+".Test"),
					mapEntry("Uint32keymapEntry", uint32Type, "."+pkg+".Test"),
					mapEntry("Uint64keymapEntry", uint64Type, "."+pkg+".Test"),
				},
			},
		},
	}

	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		t.Fatalf("protodesc.NewFile (testmessage): %v", err)
	}
	return fd.Messages().ByName("Test")
}

// ---------- BidRequest.proto descriptors + generator ----------

// buildBidRequestDescriptor constructs the descriptor for BidRequest.proto.
// CustomTypes.DecimalValue is inlined since we only need its shape.
func buildBidRequestDescriptor(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()

	stringType := descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
	messageType := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
	uint32Type := descriptorpb.FieldDescriptorProto_TYPE_UINT32.Enum()
	uint64Type := descriptorpb.FieldDescriptorProto_TYPE_UINT64.Enum()
	int64Type := descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum()
	sint32Type := descriptorpb.FieldDescriptorProto_TYPE_SINT32.Enum()
	sfixed32Type := descriptorpb.FieldDescriptorProto_TYPE_SFIXED32.Enum()
	boolType := descriptorpb.FieldDescriptorProto_TYPE_BOOL.Enum()
	floatType := descriptorpb.FieldDescriptorProto_TYPE_FLOAT.Enum()
	labelOptional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
	labelRepeated := descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()

	// CustomTypes.DecimalValue { int64 units=1; sfixed32 nanos=2; }
	decimalValueDesc := &descriptorpb.DescriptorProto{
		Name: proto.String("DecimalValue"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: proto.String("units"), Number: proto.Int32(1), Type: int64Type, Label: labelOptional},
			{Name: proto.String("nanos"), Number: proto.Int32(2), Type: sfixed32Type, Label: labelOptional},
		},
	}

	// DealExtEvent { uint32 adspottype=1; uint32 guaranteed=2; bool must_bid=3; }
	dealExtEvent := &descriptorpb.DescriptorProto{
		Name: proto.String("DealExtEvent"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: proto.String("adspottype"), Number: proto.Int32(1), Type: uint32Type, Label: labelOptional},
			{Name: proto.String("guaranteed"), Number: proto.Int32(2), Type: uint32Type, Label: labelOptional},
			{Name: proto.String("must_bid"), Number: proto.Int32(3), Type: boolType, Label: labelOptional},
		},
	}

	// DealEvent
	dealEvent := &descriptorpb.DescriptorProto{
		Name: proto.String("DealEvent"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: proto.String("id"), Number: proto.Int32(1), Type: stringType, Label: labelOptional},
			{Name: proto.String("bidfloor"), Number: proto.Int32(2), Type: messageType, TypeName: proto.String(".bidrequest.DecimalValue"), Label: labelOptional},
			{Name: proto.String("at"), Number: proto.Int32(3), Type: uint32Type, Label: labelOptional},
			{Name: proto.String("wseat"), Number: proto.Int32(4), Type: stringType, Label: labelRepeated},
			{Name: proto.String("bidfloorcur"), Number: proto.Int32(5), Type: stringType, Label: labelOptional},
			{Name: proto.String("ext"), Number: proto.Int32(6), Type: messageType, TypeName: proto.String(".bidrequest.BidRequestEvent.ImpressionEvent.PrivateMarketplaceEvent.DealEvent.DealExtEvent"), Label: labelOptional},
		},
		NestedType: []*descriptorpb.DescriptorProto{dealExtEvent},
	}

	// PrivateMarketplaceEvent
	pmpEvent := &descriptorpb.DescriptorProto{
		Name: proto.String("PrivateMarketplaceEvent"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: proto.String("private_auction"), Number: proto.Int32(1), Type: uint32Type, Label: labelOptional},
			{Name: proto.String("deals"), Number: proto.Int32(2), Type: messageType, TypeName: proto.String(".bidrequest.BidRequestEvent.ImpressionEvent.PrivateMarketplaceEvent.DealEvent"), Label: labelRepeated},
		},
		NestedType: []*descriptorpb.DescriptorProto{dealEvent},
	}

	// BannerEvent
	bannerEvent := &descriptorpb.DescriptorProto{
		Name: proto.String("BannerEvent"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: proto.String("mimes"), Number: proto.Int32(1), Type: stringType, Label: labelRepeated},
			{Name: proto.String("w"), Number: proto.Int32(2), Type: uint32Type, Label: labelOptional},
			{Name: proto.String("h"), Number: proto.Int32(3), Type: uint32Type, Label: labelOptional},
		},
	}

	// VideoEvent
	videoEvent := &descriptorpb.DescriptorProto{
		Name: proto.String("VideoEvent"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: proto.String("mimes"), Number: proto.Int32(1), Type: stringType, Label: labelRepeated},
			{Name: proto.String("w"), Number: proto.Int32(2), Type: uint32Type, Label: labelOptional},
			{Name: proto.String("h"), Number: proto.Int32(3), Type: uint32Type, Label: labelOptional},
			{Name: proto.String("minduration"), Number: proto.Int32(4), Type: uint32Type, Label: labelOptional},
			{Name: proto.String("maxduration"), Number: proto.Int32(5), Type: uint32Type, Label: labelOptional},
			{Name: proto.String("protocols"), Number: proto.Int32(6), Type: uint32Type, Label: labelRepeated},
		},
	}

	// ImpExt
	impExt := &descriptorpb.DescriptorProto{
		Name: proto.String("ImpExt"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: proto.String("displaytime"), Number: proto.Int32(1), Type: uint32Type, Label: labelOptional},
		},
	}

	// ImpQuantityEvent
	impQuantityEvent := &descriptorpb.DescriptorProto{
		Name: proto.String("ImpQuantityEvent"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: proto.String("multiplier"), Number: proto.Int32(1), Type: messageType, TypeName: proto.String(".bidrequest.DecimalValue"), Label: labelOptional},
		},
	}

	// ImpressionEvent
	impressionEvent := &descriptorpb.DescriptorProto{
		Name: proto.String("ImpressionEvent"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: proto.String("id"), Number: proto.Int32(1), Type: stringType, Label: labelOptional},
			{Name: proto.String("exp"), Number: proto.Int32(2), Type: uint32Type, Label: labelOptional},
			{Name: proto.String("pmp"), Number: proto.Int32(3), Type: messageType, TypeName: proto.String(".bidrequest.BidRequestEvent.ImpressionEvent.PrivateMarketplaceEvent"), Label: labelOptional},
			{Name: proto.String("banner"), Number: proto.Int32(4), Type: messageType, TypeName: proto.String(".bidrequest.BidRequestEvent.ImpressionEvent.BannerEvent"), Label: labelOptional},
			{Name: proto.String("video"), Number: proto.Int32(5), Type: messageType, TypeName: proto.String(".bidrequest.BidRequestEvent.ImpressionEvent.VideoEvent"), Label: labelOptional},
			{Name: proto.String("bidfloor"), Number: proto.Int32(6), Type: messageType, TypeName: proto.String(".bidrequest.DecimalValue"), Label: labelOptional},
			{Name: proto.String("bidfloorcur"), Number: proto.Int32(7), Type: stringType, Label: labelOptional},
			{Name: proto.String("ext"), Number: proto.Int32(8), Type: messageType, TypeName: proto.String(".bidrequest.BidRequestEvent.ImpressionEvent.ImpExt"), Label: labelOptional},
			{Name: proto.String("dt"), Number: proto.Int32(9), Type: uint64Type, Label: labelOptional},
			{Name: proto.String("qty"), Number: proto.Int32(10), Type: messageType, TypeName: proto.String(".bidrequest.BidRequestEvent.ImpressionEvent.ImpQuantityEvent"), Label: labelOptional},
		},
		NestedType: []*descriptorpb.DescriptorProto{pmpEvent, bannerEvent, videoEvent, impExt, impQuantityEvent},
	}

	// GeoEvent
	geoEvent := &descriptorpb.DescriptorProto{
		Name: proto.String("GeoEvent"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: proto.String("lat"), Number: proto.Int32(1), Type: messageType, TypeName: proto.String(".bidrequest.DecimalValue"), Label: labelOptional},
			{Name: proto.String("lon"), Number: proto.Int32(2), Type: messageType, TypeName: proto.String(".bidrequest.DecimalValue"), Label: labelOptional},
			{Name: proto.String("type"), Number: proto.Int32(3), Type: uint32Type, Label: labelOptional},
			{Name: proto.String("country"), Number: proto.Int32(4), Type: stringType, Label: labelOptional},
			{Name: proto.String("region"), Number: proto.Int32(5), Type: stringType, Label: labelOptional},
			{Name: proto.String("city"), Number: proto.Int32(6), Type: stringType, Label: labelOptional},
			{Name: proto.String("dma"), Number: proto.Int32(7), Type: uint32Type, Label: labelOptional},
			{Name: proto.String("utcoffset"), Number: proto.Int32(8), Type: sint32Type, Label: labelOptional},
		},
	}

	// DeviceEvent
	deviceEvent := &descriptorpb.DescriptorProto{
		Name: proto.String("DeviceEvent"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: proto.String("ifa"), Number: proto.Int32(1), Type: stringType, Label: labelOptional},
			{Name: proto.String("ua"), Number: proto.Int32(2), Type: stringType, Label: labelOptional},
			{Name: proto.String("ip"), Number: proto.Int32(3), Type: stringType, Label: labelOptional},
			{Name: proto.String("geo"), Number: proto.Int32(4), Type: messageType, TypeName: proto.String(".bidrequest.BidRequestEvent.DeviceEvent.GeoEvent"), Label: labelOptional},
			{Name: proto.String("w"), Number: proto.Int32(5), Type: uint32Type, Label: labelOptional},
			{Name: proto.String("h"), Number: proto.Int32(6), Type: uint32Type, Label: labelOptional},
		},
		NestedType: []*descriptorpb.DescriptorProto{geoEvent},
	}

	// DemographicEvent
	demographicEvent := &descriptorpb.DescriptorProto{
		Name: proto.String("DemographicEvent"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: proto.String("total"), Number: proto.Int32(1), Type: messageType, TypeName: proto.String(".bidrequest.DecimalValue"), Label: labelOptional},
		},
	}

	// UserExt
	userExt := &descriptorpb.DescriptorProto{
		Name: proto.String("UserExt"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: proto.String("demographic"), Number: proto.Int32(1), Type: messageType, TypeName: proto.String(".bidrequest.BidRequestEvent.UserEvent.UserExt.DemographicEvent"), Label: labelOptional},
		},
		NestedType: []*descriptorpb.DescriptorProto{demographicEvent},
	}

	// UserEvent
	userEvent := &descriptorpb.DescriptorProto{
		Name: proto.String("UserEvent"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: proto.String("id"), Number: proto.Int32(1), Type: stringType, Label: labelOptional},
			{Name: proto.String("ext"), Number: proto.Int32(2), Type: messageType, TypeName: proto.String(".bidrequest.BidRequestEvent.UserEvent.UserExt"), Label: labelOptional},
		},
		NestedType: []*descriptorpb.DescriptorProto{userExt},
	}

	// SitePublisher
	sitePublisher := &descriptorpb.DescriptorProto{
		Name: proto.String("SitePublisher"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: proto.String("id"), Number: proto.Int32(1), Type: stringType, Label: labelOptional},
			{Name: proto.String("name"), Number: proto.Int32(2), Type: stringType, Label: labelOptional},
		},
	}

	// SiteEvent
	siteEvent := &descriptorpb.DescriptorProto{
		Name: proto.String("SiteEvent"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: proto.String("id"), Number: proto.Int32(1), Type: stringType, Label: labelOptional},
			{Name: proto.String("name"), Number: proto.Int32(2), Type: stringType, Label: labelOptional},
			{Name: proto.String("publisher"), Number: proto.Int32(3), Type: messageType, TypeName: proto.String(".bidrequest.BidRequestEvent.SiteEvent.SitePublisher"), Label: labelOptional},
			{Name: proto.String("page"), Number: proto.Int32(4), Type: stringType, Label: labelOptional},
		},
		NestedType: []*descriptorpb.DescriptorProto{sitePublisher},
	}

	// TechnicalProviderEvent
	techProviderEvent := &descriptorpb.DescriptorProto{
		Name: proto.String("TechnicalProviderEvent"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: proto.String("id"), Number: proto.Int32(1), Type: uint32Type, Label: labelOptional},
			{Name: proto.String("name"), Number: proto.Int32(2), Type: stringType, Label: labelOptional},
		},
	}

	// PublisherEvent
	PublisherEvent := &descriptorpb.DescriptorProto{
		Name: proto.String("PublisherEvent"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: proto.String("id"), Number: proto.Int32(1), Type: stringType, Label: labelOptional},
			{Name: proto.String("name"), Number: proto.Int32(2), Type: stringType, Label: labelOptional},
		},
	}

	// BidRequestDoohEvent
	bidRequestDoohEvent := &descriptorpb.DescriptorProto{
		Name: proto.String("BidRequestDoohEvent"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: proto.String("id"), Number: proto.Int32(1), Type: stringType, Label: labelOptional},
			{Name: proto.String("name"), Number: proto.Int32(2), Type: stringType, Label: labelOptional},
			{Name: proto.String("venuetype"), Number: proto.Int32(3), Type: stringType, Label: labelRepeated},
			{Name: proto.String("venuetypetax"), Number: proto.Int32(4), Type: uint32Type, Label: labelOptional},
			{Name: proto.String("publisher"), Number: proto.Int32(5), Type: messageType, TypeName: proto.String(".bidrequest.BidRequestEvent.BidRequestDoohEvent.PublisherEvent"), Label: labelOptional},
			{Name: proto.String("audience"), Number: proto.Int32(6), Type: floatType, Label: labelOptional},
		},
		NestedType: []*descriptorpb.DescriptorProto{PublisherEvent},
	}

	// BidRequestEvent (top-level)
	bidRequestEvent := &descriptorpb.DescriptorProto{
		Name: proto.String("BidRequestEvent"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{Name: proto.String("id"), Number: proto.Int32(1), Type: stringType, Label: labelOptional},
			{Name: proto.String("user"), Number: proto.Int32(2), Type: messageType, TypeName: proto.String(".bidrequest.BidRequestEvent.UserEvent"), Label: labelOptional},
			{Name: proto.String("device"), Number: proto.Int32(3), Type: messageType, TypeName: proto.String(".bidrequest.BidRequestEvent.DeviceEvent"), Label: labelOptional},
			{Name: proto.String("imp"), Number: proto.Int32(4), Type: messageType, TypeName: proto.String(".bidrequest.BidRequestEvent.ImpressionEvent"), Label: labelRepeated},
			{Name: proto.String("site"), Number: proto.Int32(5), Type: messageType, TypeName: proto.String(".bidrequest.BidRequestEvent.SiteEvent"), Label: labelOptional},
			{Name: proto.String("cur"), Number: proto.Int32(6), Type: stringType, Label: labelRepeated},
			{Name: proto.String("tmax"), Number: proto.Int32(7), Type: uint32Type, Label: labelOptional},
			{Name: proto.String("test"), Number: proto.Int32(8), Type: uint32Type, Label: labelOptional},
			{Name: proto.String("at"), Number: proto.Int32(9), Type: uint32Type, Label: labelOptional},
			{Name: proto.String("throttled"), Number: proto.Int32(10), Type: boolType, Label: labelOptional},
			{Name: proto.String("technicalprovider"), Number: proto.Int32(12), Type: messageType, TypeName: proto.String(".bidrequest.BidRequestEvent.TechnicalProviderEvent"), Label: labelOptional},
			{Name: proto.String("dooh"), Number: proto.Int32(13), Type: messageType, TypeName: proto.String(".bidrequest.BidRequestEvent.BidRequestDoohEvent"), Label: labelOptional},
			{Name: proto.String("ad_unit_uuid"), Number: proto.Int32(14), Type: stringType, Label: labelOptional},
		},
		NestedType: []*descriptorpb.DescriptorProto{
			userEvent, deviceEvent, siteEvent, impressionEvent,
			techProviderEvent, bidRequestDoohEvent,
		},
	}

	fdp := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("BidRequest.proto"),
		Package: proto.String("bidrequest"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			decimalValueDesc,
			bidRequestEvent,
		},
	}

	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		t.Fatalf("protodesc.NewFile (BidRequest): %v", err)
	}
	return fd.Messages().ByName("BidRequestEvent")
}

// generateTestMessage uses protorand to create a *dynamicpb.Message with random
// data for the given message descriptor. Seed is fixed for reproducibility.
func generateTestMessage(t *testing.T, md protoreflect.MessageDescriptor) *dynamicpb.Message {
	t.Helper()
	pr := protorand.New()
	pr.Seed(42)
	pr.MaxCollectionElements = 3
	pr.MaxDepth = 4
	msg, err := pr.NewDynamicProtoRand(md)
	if err != nil {
		t.Fatalf("protorand.NewDynamicProtoRand(%s): %v", md.FullName(), err)
	}
	return msg
}

// ---------- Tests ----------

func TestParsePathAndValues_TestMessage(t *testing.T) {
	md := buildTestMessageDescriptor(t)
	msg := generateTestMessage(t, md)

	paths := []string{
		// scalar on nested message
		"nested.stringfield",
		"nested.intfield",
		"nested.bytesfield",
		// repeated message element -> nested scalar
		"repeats[0].nested.stringfield",
		// repeated scalar
		"int32repeats[0]",
		// deeply nested: nested -> nested (back-ref to Test) -> nested -> stringfield
		"nested.nested.nested.stringfield",
	}
	for _, pathStr := range paths {
		t.Run(pathStr, func(t *testing.T) {
			p, err := ParsePath(md, pathStr)
			if err != nil {
				t.Fatalf("ParsePath(%q): %v", pathStr, err)
			}
			t.Logf("path:  %s", p)

			results, err := PathValues(p, msg)
			if err != nil {
				t.Fatalf("PathValues(%q): %v", pathStr, err)
			}
			if len(results) == 0 {
				t.Fatalf("PathValues(%q): no results", pathStr)
			}
			vals := results[0]
			last := vals.Index(-1)
			t.Logf("value: %v", FormatValue(last.Value, last.Step.FieldDescriptor()))
			t.Logf("full:  %s", vals)
		})
	}
}

func TestParsePathAndValues_BidRequest(t *testing.T) {
	md := buildBidRequestDescriptor(t)
	msg := generateTestMessage(t, md)

	paths := []string{
		// top-level scalar
		"id",
		// nested message field
		"device.ip",
		// deeply nested: device -> geo -> country
		"device.geo.country",
		// repeated message element -> scalar
		"imp[0].id",
		// repeated message -> nested message -> scalar
		"imp[0].bidfloor.units",
		// repeated -> nested repeated: imp[0].pmp.deals[0].id
		"imp[0].pmp.deals[0].id",
		// deeply nested through deal: imp[0].pmp.deals[0].ext.must_bid
		"imp[0].pmp.deals[0].ext.must_bid",
		// repeated -> banner dimensions
		"imp[0].banner.w",
		// repeated -> video protocols (repeated scalar)
		"imp[0].video.mimes[0]",
		// deeply nested user path
		"user.ext.demographic.total.units",
		// site publisher
		"site.publisher.name",
		// dooh publisher
		"dooh.publisher.name",
		// technical provider
		"technicalprovider.name",
		// repeated string
		"cur[0]",
		// top-level bool
		"throttled",
	}
	for _, pathStr := range paths {
		t.Run(pathStr, func(t *testing.T) {
			p, err := ParsePath(md, pathStr)
			if err != nil {
				t.Fatalf("ParsePath(%q): %v", pathStr, err)
			}
			t.Logf("path:  %s", p)

			results, err := PathValues(p, msg)
			if err != nil {
				t.Fatalf("PathValues(%q): %v", pathStr, err)
			}
			if len(results) == 0 {
				t.Fatalf("PathValues(%q): no results", pathStr)
			}
			vals := results[0]
			last := vals.Index(-1)
			t.Logf("value: %v", FormatValue(last.Value, last.Step.FieldDescriptor()))
			t.Logf("full:  %s", vals)
		})
	}
}
