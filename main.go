package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/ko-han/boltdb-cli/command"
	"go.etcd.io/bbolt"
)

func init() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [OPTION] DATABASE_FILE\n", os.Args[0])
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
	ctx.SetPrompt(filename + " >> ")
	_, err = ctx.ReadHistory()
	if err != nil {
		ctx.Printf("read history fails: %v", err)
	}
	defer ctx.Close()
	for ctx.Next() {
		ctx.Do(r)
	}
}
