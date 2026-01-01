package main

import (
	"os"

	"hyper-tracker/cmd/tracker/commands"
)

func main() {
	if err := commands.Execute(); err != nil {
		os.Exit(1)
	}
}
