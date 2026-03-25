package bufarrowlib

import (
	"bytes"
	"context"
	"os"
	"testing"

	"buf.build/go/hyperpb"
	"github.com/apache/arrow-go/v18/arrow/memory"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestRead(t *testing.T) {
	msg := &metricsv1.MetricsData{}
	b := build(msg.ProtoReflect())
	b.build(memory.DefaultAllocator)
	b.append(msg.ProtoReflect())
	msg.ResourceMetrics = []*metricsv1.ResourceMetrics{
		{ScopeMetrics: []*metricsv1.ScopeMetrics{
			{Metrics: []*metricsv1.Metric{
				{
					Name: "check",
					Data: &metricsv1.Metric_Gauge{
						Gauge: &metricsv1.Gauge{
							DataPoints: []*metricsv1.NumberDataPoint{
								{
									TimeUnixNano: 16, Value: &metricsv1.NumberDataPoint_AsInt{
										AsInt: 18,
									},
									Attributes: []*commonv1.KeyValue{
										{Key: "key", Value: &commonv1.AnyValue{
											Value: &commonv1.AnyValue_StringValue{
												StringValue: "value",
											},
										}},
									},
								},
							},
						},
					},
				},
			}},
		}},
	}
	b.append(msg.ProtoReflect())

	var o bytes.Buffer
	err := b.WriteParquet(&o)
	if err != nil {
		t.Fatal(err)
	}
	write := struct{}{}
	matchBytes(t, "testdata/otel_metrics_data.parquet", o.Bytes(), write)
	matchBytes(t, "testdata/otel_metrics_data.parquet", o.Bytes())

	f, err := os.Open("testdata/otel_metrics_data.parquet")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	r, err := b.Read(context.Background(), f, nil)
	if err != nil {
		t.Fatal(err)
	}
	data, err := r.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	// fmt.Printf("arrow.MarshalJSON data: %s\n", string(data))
	match(t, "testdata/otel_metrics_data_parquet_read.json", string(data))
	md := new(metricsv1.MetricsData)
	msgType := hyperpb.CompileMessageDescriptor(md.ProtoReflect().Descriptor())
	rd := unmarshal(msgType, b.root, r, []int{1})
	data, err = protojson.Marshal(rd[0])
	if err != nil {
		t.Fatal(err)
	}
	// fmt.Printf("data: %s\n", string(data))
	matchJSON(t, "testdata/otel_metrics_data_parquet_read_decoded.json", string(data))
}

func TestWriteMultiple(t *testing.T) {
	msg := &metricsv1.MetricsData{}
	b := build(msg.ProtoReflect())
	b.build(memory.DefaultAllocator)
	b.append(msg.ProtoReflect())
	msg.ResourceMetrics = []*metricsv1.ResourceMetrics{
		{ScopeMetrics: []*metricsv1.ScopeMetrics{
			{Metrics: []*metricsv1.Metric{
				{Name: "check", Data: &metricsv1.Metric_Gauge{
					Gauge: &metricsv1.Gauge{
						DataPoints: []*metricsv1.NumberDataPoint{
							{
								TimeUnixNano: 16, Value: &metricsv1.NumberDataPoint_AsInt{
									AsInt: 18,
								},
								Attributes: []*commonv1.KeyValue{
									{Key: "key", Value: &commonv1.AnyValue{
										Value: &commonv1.AnyValue_StringValue{
											StringValue: "value",
										},
									}},
								},
							},
						},
					},
				}},
			}},
		}},
	}
	b.append(msg.ProtoReflect())

	var o bytes.Buffer
	r := b.NewRecordBatch()
	err := b.WriteParquetRecords(&o, r, r)
	if err != nil {
		t.Fatal(err)
	}
}
