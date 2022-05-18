package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ko-han/boltdb-cli/command"
	"go.etcd.io/bbolt"
)

func init() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [OPTION] <database-filename>\n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	filename := flag.Arg(0)
	if filename == "" {
		flag.Usage()
		os.Exit(1)
	}
	promptMode := flag.NArg() == 1

	if _, err := os.Stat(filename); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	db, err := bbolt.Open(filename, 0640, &bbolt.Options{
		Timeout: time.Second,
	})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	r := command.NewRegisterWithDB(db)
	ctx := command.NewContext()
	defer ctx.Close()

	if !promptMode {
		if err := r.Execute(ctx, strings.Join(flag.Args()[1:], " ")); err != nil {
			ctx.Fatalf("error: %v\n", err)
			ctx.Close()
			os.Exit(1)
		}
		return
	}

	ctx.SetPrompt(filename + " >> ")
	_, err = ctx.ReadHistory()
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		ctx.Printf("read history fails: %v", err)
	}
	defer func() {
		_, err := ctx.WriteHistory()
		if err != nil {
			ctx.Printf("write history fails: %v", err)
		}
	}()
	for ctx.Next() {
		ctx.Do(r)
	}
}
