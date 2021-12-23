package plantotrace

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/genproto/googleapis/spanner/v1"
)

const name = "spannerspan"

func nodeTitle(node *spanner.PlanNode) string {
	metadataFields := node.GetMetadata().GetFields()

	operator := joinIfNotEmpty(" ",
		metadataFields["call_type"].GetStringValue(),
		metadataFields["iterator_type"].GetStringValue(),
		strings.TrimSuffix(metadataFields["scan_type"].GetStringValue(), "Scan"),
		node.GetDisplayName(),
	)

	fields := make([]string, 0)
	for k, v := range metadataFields {
		switch k {
		case "call_type", "iterator_type": // Skip because it is displayed in node title
			continue
		case "scan_type": // Skip because it is combined with scan_target
			continue
		case "subquery_cluster_node": // Skip because it is useless
			continue
		case "scan_target":
			fields = append(fields, fmt.Sprintf("%s: %s",
				strings.TrimSuffix(metadataFields["scan_type"].GetStringValue(), "Scan"),
				v.GetStringValue()))
		default:
			fields = append(fields, fmt.Sprintf("%s: %s", k, v.GetStringValue()))
		}
	}

	sort.Strings(fields)

	return joinIfNotEmpty(" ", operator, encloseIfNotEmpty("(", strings.Join(fields, ", "), ")"))
}

func joinIfNotEmpty(sep string, input ...string) string {
	var filtered []string
	for _, s := range input {
		if s != "" {
			filtered = append(filtered, s)
		}
	}
	return strings.Join(filtered, sep)
}

func encloseIfNotEmpty(open, input, close string) string {
	if input == "" {
		return ""
	}
	return open + input + close
}

func Span(ctx context.Context, stats *spanner.ResultSetStats) {
	if stats.GetQueryPlan() != nil {
		processNode(ctx, stats.GetQueryPlan().GetPlanNodes(), stats.GetQueryPlan().GetPlanNodes()[0], nil, time.Time{}, time.Time{})
	}
}

func maxVisible(planNodes []*spanner.PlanNode) int {
	for i := len(planNodes) - 1; i >= 0; i-- {
		if isVisible(planNodes[i]) {
			return i
		}
	}
	return 0
}

func processNode(ctx context.Context, planNodes []*spanner.PlanNode, planNode *spanner.PlanNode, link *spanner.PlanNode_ChildLink, parentStart, parentEnd time.Time) {
	executionSummary, ok := planNode.GetExecutionStats().AsMap()["execution_summary"].(map[string]interface{})
	if ok {
		sStart, _ := executionSummary["execution_start_timestamp"].(string)
		executionStartTimestamp, _ := parseUnixWithFraction(sStart)
		if !executionStartTimestamp.IsZero() {
			parentStart = executionStartTimestamp
		}
		sEnd, _ := executionSummary["execution_end_timestamp"].(string)
		executionEndTimestamp, _ := parseUnixWithFraction(sEnd)
		if !executionEndTimestamp.IsZero() {
			parentEnd = executionEndTimestamp
		}

		if os.Getenv("DEBUG") != "" {
			b, _ := planNode.GetExecutionStats().MarshalJSON()
			fmt.Println(planNode.Index, executionStartTimestamp, executionEndTimestamp, string(b))
		}
	}

	if isVisible(planNode) {
		var span trace.Span
		var linkLabel string
		if t := link.GetType(); t != "" {
			linkLabel = fmt.Sprintf("[%s] ", t)
		}
		ctx, span = otel.Tracer(name).Start(ctx, fmt.Sprintf("%0*d: %s%s", len(fmt.Sprint(maxVisible(planNodes))), planNode.GetIndex(), linkLabel, nodeTitle(planNode)), trace.WithTimestamp(parentStart))
		defer span.End(trace.WithTimestamp(parentEnd))

		span.SetAttributes(attribute.Int("index", int(planNode.GetIndex())))
		for _, childLink := range planNode.GetChildLinks() {
			childNode := planNodes[childLink.GetChildIndex()]
			if childNode.GetDisplayName() == "Function" && (strings.HasSuffix(childLink.GetType(), "Condition") || childLink.GetType() == "Split Range") {
				span.SetAttributes(attribute.String(childLink.GetType(), childNode.GetShortRepresentation().GetDescription()))
			}
		}

		for _, childLink := range planNode.GetChildLinks() {
			processNode(ctx, planNodes, planNodes[childLink.GetChildIndex()], childLink, parentStart, parentEnd)
		}
	}
}

func isVisible(planNode *spanner.PlanNode) bool {
	return planNode.GetKind() == spanner.PlanNode_RELATIONAL || strings.HasSuffix(planNode.GetDisplayName(), "Subquery")
}

func parseUnixWithFraction(s string) (time.Time, error) {
	ss := strings.SplitN(s, ".", 2)
	if len(ss) != 2 {
		return time.Time{}, errors.New("ss is not valid format")
	}

	secStr, fracStr := ss[0], ss[1]

	sec, err := strconv.Atoi(secStr)
	if err != nil {
		return time.Time{}, err
	}

	const fracPrecision = 9
	if len(fracStr) > fracPrecision {
		return time.Time{}, fmt.Errorf("fraction part should be shorter or equal than %d, actual: %d", fracPrecision, len(fracStr))
	}
	nsec, err := strconv.Atoi(fracStr + strings.Repeat("0", fracPrecision-len(fracStr)))
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(int64(sec), int64(nsec)), nil
}
