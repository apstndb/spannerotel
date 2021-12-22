package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"cloud.google.com/go/spanner"
	texporter "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace"
	"github.com/apstndb/spannerotel/interceptor"
	octrace "go.opencensus.io/trace"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	oteloc "go.opentelemetry.io/otel/bridge/opencensus"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

const (
	service     = "spanner-plan"
	environment = "test"
	id          = 1
)

var mode = flag.String("mode", "", "")
var sql = flag.String("sql", "", "")
var cloudOperationsProject = flag.String("cloud-operations-project", "", "")

func oltpTracerProvider() (sdktrace.SpanExporter, error) {
	client := otlptracegrpc.NewClient(otlptracegrpc.WithInsecure())

	exporter, err := otlptrace.New(context.Background(), client)
	if err != nil {
		return nil, fmt.Errorf("otlptrace.New: %v", err)
	}
	return exporter, err
}

func cloudOperationsTracerProvider() (sdktrace.SpanExporter, error) {
	exporter, err := texporter.New(texporter.WithProjectID(*cloudOperationsProject))
	if err != nil {
		return nil, fmt.Errorf("texporter.New: %v", err)
	}
	return exporter, err
}

// TracerProvider returns an OpenTelemetry TracerProvider configured to use
// the Jaeger exporter that will send spans to the provided url. The returned
// TracerProvider will also use a Resource configured with all the information
// about the application.
func TracerProvider(mode, url string) (*sdktrace.TracerProvider, error) {
	var exp sdktrace.SpanExporter
	var err error
	switch mode {
	case "datadog":
		exp, err = oltpTracerProvider()
	case "stackdriver":
		exp, err = cloudOperationsTracerProvider()
	default:
		exp, err = jaegerTracerProvider(url)
	}
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		// Always be sure to batch in production.
		sdktrace.WithBatcher(exp),
		// Record information about this application in an Resource.
		// sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(service),
			attribute.String("environment", environment),
			attribute.Int64("ID", id),
		)),
		// sdktrace.WithSampler(sdktrace.AlwaysSample()),
		// sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.NeverSample())),
		// sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.NeverSample())),
	)
	return tp, nil
}

func jaegerTracerProvider(url string) (sdktrace.SpanExporter, error) {
	// Create the Jaeger exporter
	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(url)))
	if err != nil {
		return nil, fmt.Errorf("jaeger.New: %v", err)
	}
	return exp, err
}

func main() {
if err := run(context.Background()); err != nil {
	log.Fatalln(err)
}
}

func run(ctx context.Context) error {
	flag.Parse()
	tp, err := TracerProvider(*mode, "http://localhost:14268/api/traces")
	if err != nil {
		log.Fatal(err)
	}

	otel.SetTracerProvider(tp)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Cleanly shutdown and flush telemetry when the application exits.
	defer func(ctx context.Context) {
		// Do not make the application hang when it is shutdown.
		ctx, cancel = context.WithTimeout(ctx, time.Second*5)
		defer cancel()
		if err := tp.Shutdown(ctx); err != nil {
			log.Fatal(err)
		}
	}(ctx)

	octrace.DefaultTracer = oteloc.NewTracer(tp.Tracer("bridge"))

	databaseStr := fmt.Sprintf("projects/%s/instances/%s/databases/%s", os.Getenv("CLOUDSDK_CORE_PROJECT"), os.Getenv("CLOUDSDK_SPANNER_INSTANCE"), os.Getenv("DATABASE_ID"))
	client, err := spanner.NewClientWithConfig(ctx, databaseStr, spanner.ClientConfig{
		SessionPoolConfig: spanner.SessionPoolConfig{
			MaxOpened:           1,
			MinOpened:           1,
			TrackSessionHandles: true,
		},
	}, []option.ClientOption{
		// option.WithGRPCDialOption(grpc.WithChainUnaryInterceptor(UnaryInterceptor(pp.c))),
		option.WithGRPCDialOption(grpc.WithChainStreamInterceptor(interceptor.StreamInterceptor(interceptor.WithDefaultDecorators()))),
	}...)
	if err != nil {
		return err
	}

	for i := 0; i < 5; i++ {
		err := func() error {
			tracer := otel.Tracer("trace")
			ctx, sp := tracer.Start(ctx, "exec")
			defer sp.End()
			// Enforce sampled
			// sp.SpanContext().WithTraceFlags(sp.SpanContext().TraceFlags().WithSampled(true))

			return client.Single().QueryWithStats(ctx, spanner.NewStatement(*sql)).Do(func(r *spanner.Row) error {
				return nil
			})
		}()
		if err != nil {
			return err
		}
	}
	return nil
}

