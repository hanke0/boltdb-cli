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

func commandStat(db *bolt.DB, ctx *Context, args []string) error {
	var tab = table.Table{
		Headers: []string{"bucket", "keys", "nested-buckets", "depth"},
	}
	var total int64
	var buckets int64
	err := db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			s := b.Stats()
			tab.Rows = append(tab.Rows, []string{
				safeBytesToString(name),
				fmt.Sprintf("%d", s.KeyN),
				fmt.Sprintf("%d", s.BucketN),
				fmt.Sprintf("%d", s.Depth),
			})
			total += int64(s.KeyN)
			buckets++
			return nil
		})
	})
	if err != nil {
		return err
	}
	tab.Rows = append(tab.Rows, []string{
		"-total-",
		fmt.Sprintf("%d", total),
		fmt.Sprintf("%d", buckets),
		"-",
	})
	return tab.WriteTable(ctx.Output(), table.DefaultConfig())
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

func commandListBucket(db *bolt.DB, ctx *Context, args []string) error {
	var i int64
	const askContinueSize = 32
	var errExit = errors.New("exit")
	err := db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			i++
			ctx.Printf("%-8d %s\n", i, safeBytesToString(name))
			if i%askContinueSize == 0 && i >= askContinueSize {
				if !askContinue(ctx) {
					return errExit
				}
			}
			return nil
		})
	})
	if err == errExit { // nolint:errorlint
		return nil
	}
	return err
}

func commandListBucketKeys(db *bolt.DB, ctx *Context, args []string) error {
	var i int64
	const askContinueSize = 32
	var errExit = errors.New("exit")
	err := db.View(func(tx *bolt.Tx) error {
		bu := tx.Bucket(stringToBytes(args[0]))
		if bu == nil {
			ctx.Printf("err: bucket not found")
			return nil
		}
		return bu.ForEach(func(k, v []byte) error {
			i++
			if len(args) > 1 {
				ctx.Printf("%-8d %s  %s\n", i, safeBytesToString(k), safeBytesToString(v))
			} else {
				ctx.Printf("%-8d %s\n", i, safeBytesToString(k))
			}
			if i%askContinueSize == 0 && i >= askContinueSize {
				if !askContinue(ctx) {
					return errExit
				}
			}
			return nil
		})
	})
	if err == errExit { // nolint:errorlint
		return nil
	}
	return err
}
