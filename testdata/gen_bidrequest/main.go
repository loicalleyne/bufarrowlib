// gen_bidrequest generates a reproducible corpus of ~500 BidRequestEvent
// messages (protorand Seed(42)) plus hand-crafted edge cases, and writes
// them as length-prefixed proto bytes to testdata/bidrequest_bench.bin.
//
// Run:
//
//	go run ./testdata/gen_bidrequest/
package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"

	"github.com/loicalleyne/bufarrowlib/gen/go/samples"
	"github.com/loicalleyne/bufarrowlib/gen/go/samples/custom"
	"github.com/sryoya/protorand"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func main() {
	_, thisFile, _, _ := runtime.Caller(0)
	outDir := filepath.Dir(filepath.Dir(thisFile)) // testdata/
	outPath := filepath.Join(outDir, "bidrequest_bench.bin")

	pr := protorand.New()
	pr.Seed(42)
	pr.MaxCollectionElements = 3
	pr.MaxDepth = 5

	var messages []proto.Message

	// ── 500 random messages ──────────────────────────────────────────
	for i := 0; i < 500; i++ {
		m, err := pr.Gen(&samples.BidRequestEvent{})
		if err != nil {
			log.Fatalf("gen message %d: %v", i, err)
		}
		messages = append(messages, m)
	}

	// ── Edge cases ───────────────────────────────────────────────────
	messages = append(messages, edgeCases()...)

	// ── Write length-prefixed ────────────────────────────────────────
	f, err := os.Create(outPath)
	if err != nil {
		log.Fatalf("create %s: %v", outPath, err)
	}
	defer f.Close()

	var totalBytes int64
	for i, m := range messages {
		b, err := proto.Marshal(m)
		if err != nil {
			log.Fatalf("marshal message %d: %v", i, err)
		}
		var lenBuf [4]byte
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(b)))
		if _, err := f.Write(lenBuf[:]); err != nil {
			log.Fatal(err)
		}
		if _, err := f.Write(b); err != nil {
			log.Fatal(err)
		}
		totalBytes += int64(4 + len(b))
	}

	fmt.Printf("wrote %d messages (%d random + %d edge cases) to %s  [%.1f KB]\n",
		len(messages), 500, len(messages)-500, outPath, float64(totalBytes)/1024)
}

func edgeCases() []proto.Message {
	var msgs []proto.Message

	// ── 1. Empty deals list ──────────────────────────────────────────
	// Stresses Coalesce / null-padding: imp exists but has zero deals,
	// so the deal column should get a single null row per message.
	msgs = append(msgs, &samples.BidRequestEvent{
		Id: "edge-empty-deals",
		Imp: []*samples.BidRequestEvent_ImpressionEvent{{
			Id:     "imp-ed",
			Banner: &samples.BidRequestEvent_ImpressionEvent_BannerEvent{W: 300, H: 250},
			Pmp:    &samples.BidRequestEvent_ImpressionEvent_PrivateMarketplaceEvent{},
		}},
		User: &samples.BidRequestEvent_UserEvent{Id: "user-ed"},
		Site: &samples.BidRequestEvent_SiteEvent{
			Id:        "site-ed",
			Publisher: &samples.BidRequestEvent_SiteEvent_SitePublisher{Id: proto.String("pub-ed")},
		},
		Timestamp:         &timestamppb.Timestamp{Seconds: 1700000000, Nanos: 500000000},
		Technicalprovider: &samples.BidRequestEvent_TechnicalProviderEvent{Id: 42, Name: "tp-ed"},
	})

	// ── 2. Nil banner, populated video ───────────────────────────────
	// Stresses Cond / Coalesce fallback: width/height must come from Video.
	msgs = append(msgs, &samples.BidRequestEvent{
		Id: "edge-nil-banner",
		Imp: []*samples.BidRequestEvent_ImpressionEvent{{
			Id:    "imp-nb",
			Video: &samples.BidRequestEvent_ImpressionEvent_VideoEvent{W: 640, H: 480, Minduration: 5, Maxduration: 30},
			Pmp: &samples.BidRequestEvent_ImpressionEvent_PrivateMarketplaceEvent{
				Deals: []*samples.BidRequestEvent_ImpressionEvent_PrivateMarketplaceEvent_DealEvent{
					{Id: "deal-nb-1"},
				},
			},
		}},
		User:              &samples.BidRequestEvent_UserEvent{Id: "user-nb"},
		Site:              &samples.BidRequestEvent_SiteEvent{Id: "site-nb"},
		Timestamp:         &timestamppb.Timestamp{Seconds: 1700000001},
		Technicalprovider: &samples.BidRequestEvent_TechnicalProviderEvent{Id: 7},
	})

	// ── 3. Nil video, populated banner ───────────────────────────────
	// Happy path for banner dimensions; video getters return zero.
	msgs = append(msgs, &samples.BidRequestEvent{
		Id: "edge-nil-video",
		Imp: []*samples.BidRequestEvent_ImpressionEvent{{
			Id:     "imp-nv",
			Banner: &samples.BidRequestEvent_ImpressionEvent_BannerEvent{W: 728, H: 90},
			Pmp: &samples.BidRequestEvent_ImpressionEvent_PrivateMarketplaceEvent{
				Deals: []*samples.BidRequestEvent_ImpressionEvent_PrivateMarketplaceEvent_DealEvent{
					{Id: "deal-nv-1"},
					{Id: "deal-nv-2"},
				},
			},
		}},
		Device:            &samples.BidRequestEvent_DeviceEvent{Ifa: "device-ifa-nv"},
		Timestamp:         &timestamppb.Timestamp{Seconds: 1700000002, Nanos: 999999999},
		Technicalprovider: &samples.BidRequestEvent_TechnicalProviderEvent{Id: 99},
	})

	// ── 4. Both banner AND video nil ─────────────────────────────────
	// Double-null Cond: both width/height sources are zero.
	msgs = append(msgs, &samples.BidRequestEvent{
		Id: "edge-both-nil",
		Imp: []*samples.BidRequestEvent_ImpressionEvent{{
			Id: "imp-bn",
			Pmp: &samples.BidRequestEvent_ImpressionEvent_PrivateMarketplaceEvent{
				Deals: []*samples.BidRequestEvent_ImpressionEvent_PrivateMarketplaceEvent_DealEvent{
					{Id: "deal-bn-1"},
				},
			},
		}},
		User:      &samples.BidRequestEvent_UserEvent{Id: "user-bn"},
		Timestamp: &timestamppb.Timestamp{Seconds: 1700000003},
	})

	// ── 5. Zero-value timestamps ─────────────────────────────────────
	// Timestamp present but all-zero (epoch); tests zero propagation.
	msgs = append(msgs, &samples.BidRequestEvent{
		Id: "edge-zero-ts",
		Imp: []*samples.BidRequestEvent_ImpressionEvent{{
			Id:     "imp-zt",
			Banner: &samples.BidRequestEvent_ImpressionEvent_BannerEvent{W: 320, H: 50},
		}},
		User:      &samples.BidRequestEvent_UserEvent{Id: "user-zt"},
		Timestamp: &timestamppb.Timestamp{Seconds: 0, Nanos: 0},
	})

	// ── 6. Max-depth nesting ─────────────────────────────────────────
	// All sub-messages populated at deepest level; exercises every path
	// segment the benchmarks traverse.
	msgs = append(msgs, &samples.BidRequestEvent{
		Id: "edge-max-depth",
		User: &samples.BidRequestEvent_UserEvent{
			Id: "user-md",
			Ext: &samples.BidRequestEvent_UserEvent_UserExt{
				Demographic: &samples.BidRequestEvent_UserEvent_UserExt_DemographicEvent{
					Total: &custom.DecimalValue{Units: 12345, Nanos: 678900000},
				},
			},
		},
		Device: &samples.BidRequestEvent_DeviceEvent{
			Ifa: "device-ifa-md",
			Ua:  "Mozilla/5.0",
			Ip:  "192.168.1.1",
			Geo: &samples.BidRequestEvent_DeviceEvent_GeoEvent{
				Lat:     &custom.DecimalValue{Units: 40, Nanos: 712800000},
				Lon:     &custom.DecimalValue{Units: -74, Nanos: 5900000},
				Country: "US", Region: "NY", City: "New York",
			},
			W: 1920, H: 1080,
		},
		Imp: []*samples.BidRequestEvent_ImpressionEvent{{
			Id:     "imp-md",
			Banner: &samples.BidRequestEvent_ImpressionEvent_BannerEvent{W: 970, H: 250, Mimes: []string{"image/jpeg", "image/png"}},
			Video:  &samples.BidRequestEvent_ImpressionEvent_VideoEvent{W: 1280, H: 720, Minduration: 15, Maxduration: 60, Mimes: []string{"video/mp4"}, Protocols: []uint32{1, 2, 3}},
			Pmp: &samples.BidRequestEvent_ImpressionEvent_PrivateMarketplaceEvent{
				PrivateAuction: 1,
				Deals: []*samples.BidRequestEvent_ImpressionEvent_PrivateMarketplaceEvent_DealEvent{
					{
						Id:       "deal-md-1",
						Bidfloor: &custom.DecimalValue{Units: 5, Nanos: 500000000},
						At:       2,
						Wseat:    []string{"seat-a", "seat-b"},
						Ext: &samples.BidRequestEvent_ImpressionEvent_PrivateMarketplaceEvent_DealEvent_DealExtEvent{
							Adspottype: 1, Guaranteed: 1, MustBid: true,
						},
					},
					{
						Id:       "deal-md-2",
						Bidfloor: &custom.DecimalValue{Units: 10, Nanos: 0},
						At:       1,
						Ext: &samples.BidRequestEvent_ImpressionEvent_PrivateMarketplaceEvent_DealEvent_DealExtEvent{
							Adspottype: 2,
						},
					},
					{
						Id: "deal-md-3",
						At: 3,
					},
				},
			},
			Bidfloor: &custom.DecimalValue{Units: 1, Nanos: 250000000},
			Ext:      &samples.BidRequestEvent_ImpressionEvent_ImpExt{Displaytime: proto.Uint32(30)},
			Qty:      &samples.BidRequestEvent_ImpressionEvent_ImpQuantityEvent{Multiplier: &custom.DecimalValue{Units: 2, Nanos: 0}},
		}},
		Site: &samples.BidRequestEvent_SiteEvent{
			Id:        "site-md",
			Name:      "Example Site",
			Publisher: &samples.BidRequestEvent_SiteEvent_SitePublisher{Id: proto.String("pub-md"), Name: proto.String("Publisher Inc")},
			Page:      proto.String("https://example.com/page"),
		},
		Cur:               []string{"USD", "EUR"},
		Tmax:              proto.Uint32(200),
		Timestamp:         &timestamppb.Timestamp{Seconds: 1700000099, Nanos: 123456789},
		Technicalprovider: &samples.BidRequestEvent_TechnicalProviderEvent{Id: 1, Name: "MaxDepthTP"},
		Dooh: &samples.BidRequestEvent_BidRequestDoohEvent{
			Id: "dooh-md", Name: "Screen-A", Venuetype: []string{"transit", "retail"},
			Venuetypetax: 1, Audience: 1500.5,
			Publisher: &samples.BidRequestEvent_BidRequestDoohEvent_PublisherEvent{Id: "dooh-pub", Name: "OOH Publisher"},
		},
	})

	return msgs
}
