package wordcount

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"
)

var (
	wordRE     = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)
	empty      = beam.NewCounter("extract", "emptyLines")
	lineLen    = beam.NewDistribution("extract", "lineLenDistro")
	smallWords = beam.NewCounter("extract", "smallWords")
)

func Words(s beam.Scope, lines beam.PCollection) beam.PCollection {
	s = s.Scope("CountWords")

	// Convert lines of text into individual words.
	col := beam.ParDo(s, extractFn, lines)

	// Count the number of times each word occurs.
	return stats.Count(s, col)
}

func extractFn(ctx context.Context, line string, emit func(string)) {
	lineLen.Update(ctx, int64(len(line)))
	if len(strings.TrimSpace(line)) == 0 {
		empty.Inc(ctx, 1)
	}
	for _, word := range wordRE.FindAllString(line, -1) {
		if len(word) < 6 {
			smallWords.Inc(ctx, 1)
		}
		emit(word)
	}
}

func Format(s beam.Scope, counted beam.PCollection) beam.PCollection {
	return beam.ParDo(s, formatFn, counted)
}

func formatFn(w string, c int) string {
	return fmt.Sprintf("%s: %v", w, c)
}
