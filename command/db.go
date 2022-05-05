package command

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/tomlazar/table"
	bolt "go.etcd.io/bbolt"
)

func NewRegisterWithDB(db *bolt.DB) Register {
	r := NewRegister()
	r.Register(&dbCommand{
		db:        db,
		alias:     []string{"stat", "st"},
		help:      "Print database basic information",
		validates: NewValidats().NumArgs(0),
		executor:  commandStat,
	})
	r.Register(&dbCommand{
		db:        db,
		alias:     []string{"get", "g"},
		help:      "Get key-value pairs",
		validates: NewValidats().MinArgs(2).MaxArgs(1024),
		executor:  commandGet,
	})
	r.Register(&dbCommand{
		db:        db,
		alias:     []string{"buckets", "b"},
		help:      "List all buckets",
		validates: NewValidats().NumArgs(0),
		executor:  commandListBucket,
	})
	r.Register(&dbCommand{
		db:        db,
		alias:     []string{"keys", "k"},
		help:      "List all buckets",
		validates: NewValidats().MinArgs(1).MaxArgs(2).Choices(1, []string{"withvalue"}),
		executor:  commandListBucketKeys,
	})
	return r
}

func safeBytesToString(b []byte) string {
	a := fmt.Sprintf("%q", b)
	r := strings.NewReplacer("\n", "\\x0a", "\r", "\\x0d", "\\\"", "\"", "\"", "")
	return r.Replace(a)
}

var replaceRE = regexp.MustCompile(`\\x[a-z0-9]{2}`)

func stringToBytes(s string) []byte {
	return replaceRE.ReplaceAllFunc([]byte(s), func(b []byte) []byte {
		a, _ := strconv.ParseUint(string(b[2:]), 16, 8)
		return []byte{byte(a)}
	})
}

type dbCommand struct {
	db        *bolt.DB
	help      string
	alias     []string
	validates Validates
	executor  func(db *bolt.DB, ctx *Context, args []string) error
}

func (g *dbCommand) Alias() []string {
	return g.alias
}

func (g *dbCommand) Help() string {
	return g.help
}

func (g *dbCommand) Check(ctx *Context, args []string) error {
	return g.validates.Finish()(ctx, args)
}

func (g *dbCommand) Execute(ctx *Context, args []string) error {
	return g.executor(g.db, ctx, args)
}

const (
	maxRows         = 128
	askContinueSize = 32
)

func commandStat(db *bolt.DB, ctx *Context, args []string) error {
	var tab = table.Table{
		Headers: []string{"keys", "bucket", "max-btree-depth"},
	}
	var total, buckets, maxDepth int64
	err := db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			s := b.Stats()
			total += int64(s.KeyN)
			buckets++
			if s.Depth > int(maxDepth) {
				maxDepth = int64(s.Depth)
			}
			return nil
		})
	})
	if err != nil {
		return err
	}
	tab.Rows = append(tab.Rows, []string{
		fmt.Sprintf("%d", total),
		fmt.Sprintf("%d", buckets),
		fmt.Sprintf("%d", maxDepth),
	})
	cfg := table.DefaultConfig()
	cfg.ShowIndex = false
	return tab.WriteTable(ctx.Output(), cfg)
}

func commandGet(db *bolt.DB, ctx *Context, args []string) error {
	return db.View(func(tx *bolt.Tx) error {
		bu := tx.Bucket(stringToBytes(args[0]))
		if bu == nil {
			ctx.Printf("err: bucket not found\n")
			return nil
		}
		if len(args)-2 > 0 {
			for _, b := range args[1 : len(args)-1] {
				bu = bu.Bucket(stringToBytes(b))
				if bu == nil {
					ctx.Printf("err: bucket %s not found\n", b)
					return nil
				}
			}
		}
		v := bu.Get(stringToBytes(args[len(args)-1]))
		if v == nil {
			ctx.Printf("err: key-value not found\n")
		} else {
			ctx.Printf(safeBytesToString(v))
		}
		return nil
	})
}

func askContinue(ctx *Context) bool {
	for {
		line, err := ctx.ReadLine("continue [Y/n]?")
		if err != nil {
			return false
		}
		switch line {
		case "y", "yes", "Y", "YES", "Yes", "":
			return true
		case "n", "no", "N", "No", "NO":
			return false
		default:
			ctx.Printf("Invalid input\n")
		}
	}
}

type tablePrinter struct {
	tb    table.Table
	total int
}

func (t *tablePrinter) add(ctx *Context, v []string) bool {
	t.total++

	r := []string{
		fmt.Sprintf("%d", t.total),
	}
	r = append(r, v...)
	t.tb.Rows = append(t.tb.Rows, r)
	if t.total%askContinueSize == 0 && t.total >= askContinueSize {
		t.out(ctx)
		return askContinue(ctx)
	}
	return true
}

func (t *tablePrinter) out(ctx *Context) {
	if len(t.tb.Rows) == 0 {
		return
	}
	cfg := table.DefaultConfig()
	cfg.ShowIndex = false
	_ = t.tb.WriteTable(ctx.Output(), cfg)
	t.tb.Rows = t.tb.Rows[:0]
}

func newTablePrinter(headers []string) *tablePrinter {
	h := []string{"id"}
	h = append(h, headers...)
	return &tablePrinter{
		tb: table.Table{
			Headers: h,
		},
	}
}

var errExit = errors.New("exit")

func commandListBucket(db *bolt.DB, ctx *Context, args []string) error {
	tl := newTablePrinter([]string{"bucket", "keys", "depth"})
	err := db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			s := b.Stats()
			if !tl.add(ctx, []string{
				safeBytesToString(name),
				fmt.Sprintf("%d", s.KeyN),
				fmt.Sprintf("%d", s.Depth),
			}) {
				return errExit
			}
			return nil
		})
	})
	tl.out(ctx)
	if err == errExit { // nolint:errorlint
		return nil
	}
	return err
}

func commandListBucketKeys(db *bolt.DB, ctx *Context, args []string) error {
	hasValue := len(args) > 1
	var tl *tablePrinter
	if hasValue {
		tl = newTablePrinter([]string{"key", "value"})
	} else {
		tl = newTablePrinter([]string{"key"})
	}

	err := db.View(func(tx *bolt.Tx) error {
		bu := tx.Bucket(stringToBytes(args[0]))
		if bu == nil {
			ctx.Printf("err: bucket not found")
			return nil
		}
		return bu.ForEach(func(k, v []byte) error {
			r := []string{safeBytesToString(v)}
			if hasValue {
				r = append(r, safeBytesToString(v))
			}
			if !tl.add(ctx, r) {
				return errExit
			}
			return nil
		})
	})
	tl.out(ctx)
	if err == errExit { // nolint:errorlint
		return nil
	}
	return err
}
