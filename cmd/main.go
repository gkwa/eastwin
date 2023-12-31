package main

import (
	"os"

	"github.com/taylormonacelli/eastwin"
)

func main() {
	code := eastwin.Execute()
	os.Exit(code)
}
