// dbhealth watches databases and reports whether they are serving, how close
// they are to their limits, and whether their tables are current.
//
//	dbhealth validate -c dbhealth.yml
//	dbhealth run -c dbhealth.yml
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/turbolytics/dbhealth/internal/config"
)

const usage = `usage:
  dbhealth validate -c <file>   check the file and say how many databases it names
  dbhealth run      -c <file>   watch them and report
`

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		return 2
	}
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	path := fs.String("c", "", "the config file")
	if err := fs.Parse(args[1:]); err != nil {
		return 2
	}
	if *path == "" {
		fmt.Fprintln(os.Stderr, "dbhealth: -c <file> is required")
		return 2
	}
	f, err := config.Load(*path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dbhealth: %v\n", err)
		return 1
	}
	switch args[0] {
	case "validate":
		fmt.Printf("ok: %d databases\n", len(f.Databases))
		return 0
	case "run":
		fmt.Fprintln(os.Stderr, "not yet: run lands in a later task")
		return 2
	default:
		fmt.Fprint(os.Stderr, usage)
		return 2
	}
}
