package main

import (
	"context"
	"log"
	"regexp"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/direct"

	_ "github.com/apache/beam/sdks/go/pkg/beam/io/filesystem/local"
)

func main() {
	beam.Init()
	p := beam.NewPipeline()
	s := p.Root()
	//10 OMIT
	lines := textio.Read(s, "../../testdata/inventory*.csv")
	cleaned := beam.ParDo(s, cleanFn, lines) // HL
	textio.Write(s, "output.csv", cleaned)
	if err := direct.Execute(context.Background(), p); err != nil {
		log.Printf("pipeline execution error: %v", err)
	}
	//20 OMIT
}

//30 OMIT
var idRE = regexp.MustCompile(`\w{3}\d{6}`)

func cleanFn(line string, emit func(string)) {
	if idRE.FindStringIndex(line) == nil {
		return
	}
	emit(line)
}

//40 OMIT
