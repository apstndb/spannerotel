package interceptor

import (
	"context"
	"io"
	"strconv"
	"strings"

	plantotrace "github.com/apstndb/spannerotel/internal/plantotrace"
	"google.golang.org/grpc/metadata"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/genproto/googleapis/spanner/v1"
	"google.golang.org/grpc"
)

type interceptorOption struct {
	statsSpanDecorators []StatsSpanDecorator
	headerSpanDecorators []HeaderSpanDecorator
}

type Option func (*interceptorOption)

func WithDefaultDecorators() Option {
	return func(option *interceptorOption) {
		WithStatsSpanDecorators(queryTextSpanDecorator, elapsedTimeSpanDecorator)(option)
		WithHeaderSpanDecorators(gfeServerTimingSpanDecorator)(option)
	}
}

func WithStatsSpanDecorators(decorators ...StatsSpanDecorator) Option {
	return func(o *interceptorOption) {
		o.statsSpanDecorators = append(o.statsSpanDecorators, decorators...)
	}
}

func WithHeaderSpanDecorators(decorators ...HeaderSpanDecorator) Option {
	return func(o *interceptorOption) {
		o.headerSpanDecorators = append(o.headerSpanDecorators, decorators...)
	}
}

func StreamInterceptor(opts ...Option) func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	var o interceptorOption
	for _, option := range opts {
		option(&o)
	}
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		stream, err := streamer(ctx, desc, cc, method, opts...)
		return &ClientStream{stream, ctx, method, desc, o.statsSpanDecorators, o.headerSpanDecorators}, err
	}
}

type ClientStream struct {
	grpc.ClientStream
	ctx    context.Context
	method string
	desc   *grpc.StreamDesc
	statsSpanDecorators  []StatsSpanDecorator
	headerSpanDecorators []HeaderSpanDecorator
}

func (l *ClientStream) RecvMsg(m interface{}) error {
	err := l.ClientStream.RecvMsg(m)
	if err != nil && err != io.EOF {
		return err
	}

	ctx := l.ClientStream.Context()
	sp := trace.SpanFromContext(ctx)
	var stats *spanner.ResultSetStats
	switch m := m.(type) {
	case *spanner.PartialResultSet:
		stats = m.GetStats()
	case *spanner.ResultSet:
		stats = m.GetStats()
	}
	if stats != nil {
		 for _, dec := range l.statsSpanDecorators {
			 dec(ctx, sp, stats)
		 }
		 plantotrace.Span(ctx, stats)
	}

	// don't override RecvMsg err
	if md, err := l.ClientStream.Header(); err == nil {
	// if md, _ := l.ClientStream.Header(); md.Len() > 0 {
		for _, dec := range l.headerSpanDecorators {
			dec(ctx, sp, md)
		}
	}
	// }

	return err
}

type StatsSpanDecorator func (ctx context.Context, span trace.Span, stats *spanner.ResultSetStats)
type HeaderSpanDecorator func (ctx context.Context, span trace.Span, header metadata.MD)

type serverTiming struct {
	Name string
	DurationMs int
	Extra map[string]string
}

func split2(s, sep string) (head, rest string) {
	ss := strings.SplitN(s, sep, 2)
	if len(ss) > 1 {
		return ss[0], ss[1]
	} else {
		return ss[0], ""
	}
}

func parseServerTiming(raw string) serverTiming {
	var duration int
	name, rest := split2(raw, ";")
	extra := make(map[string]string)
	for _, v := range strings.Split(rest, ";") {
		key, value := split2(strings.TrimSpace(v), "=")
		if strings.ToLower(key) == "dur" {
			if d, err := strconv.Atoi(value); err != nil {
				continue
			} else {
				duration = d
			}
		} else {
			extra[key] = value
		}
	}
	return serverTiming{
		Name: name,
		DurationMs: duration,
		Extra: extra,
	}
}

const gfeServerTimingName = "gfet4t7"

func gfeServerTimingSpanDecorator(ctx context.Context, span trace.Span, header metadata.MD) {
	for _, rawServerTiming := range header.Get("server-timing") {
		if serverTiming := parseServerTiming(rawServerTiming); serverTiming.Name == gfeServerTimingName {
			span.SetAttributes(attribute.Int("gfe-server-timing", serverTiming.DurationMs))
		}
	}
}
func queryTextSpanDecorator(ctx context.Context, span trace.Span, stats *spanner.ResultSetStats) {
	span.SetAttributes(attribute.String("query_text", stats.GetQueryStats().GetFields()["query_text"].GetStringValue()))
}

func elapsedTimeSpanDecorator(ctx context.Context, span trace.Span, stats *spanner.ResultSetStats) {
	span.SetAttributes(attribute.String("elapsed_time", stats.GetQueryStats().GetFields()["elapsed_time"].GetStringValue()))
}

