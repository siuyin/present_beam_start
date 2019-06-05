package main

import (
	"context"
	"log"

	//10 OMIT
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/direct"

	_ "github.com/apache/beam/sdks/go/pkg/beam/io/filesystem/local" // (1)
	//20 OMIT
)

func main() {
	//30 OMIT
	beam.Init() // (2)
	p := beam.NewPipeline()
	s := p.Root()
	lines := textio.Read(s, "../../testdata/inventory*.csv") // (3)
	textio.Write(s, "output.csv", lines)
	if err := direct.Execute(context.Background(), p); err != nil { // (4)
		log.Printf("pipeline execution error: %v", err)
	}
	//40 OMIT
}
