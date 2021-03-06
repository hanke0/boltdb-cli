package command

import (
	"bytes"
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
		usage:     "get <bucket-name> [nest-bucket-name...] <key>",
		validates: NewValidats().MinArgs(2).MaxArgs(1024),
		executor:  commandGet,
	})
	r.Register(&dbCommand{
		db:        db,
		alias:     []string{"buckets", "b"},
		help:      "List all buckets",
		usage:     "buckets [pattern]",
		validates: NewValidats().MaxArgs(1),
		executor:  commandListBucket,
	})
	r.Register(&dbCommand{
		db:        db,
		alias:     []string{"keys", "k"},
		help:      "List a bucket all keys",
		usage:     "keys <bucket-name> [pattern] [withvalue]",
		validates: NewValidats().MinArgs(1).MaxArgs(3),
		executor:  commandListBucketKeys,
	})
	r.Register(&dbCommand{
		db:        db,
		alias:     []string{"remove", "rm"},
		help:      "Remove bucket or keys",
		usage:     "remove bucket|key [bucket-name] [key]",
		validates: NewValidats().MinArgs(2).Choices(0, []string{"bucket", "key"}),
		executor:  commandRemove,
	})
	r.Register(&dbCommand{
		db:        db,
		alias:     []string{"copy-bucket", "cpbkt"},
		help:      "Copy bucket to another bucket",
		usage:     "copy-bucket <dst> <src>",
		validates: NewValidats().NumArgs(2),
		executor:  commandRemove,
	})
	r.Register(&dbCommand{
		db:        db,
		alias:     []string{"set", "s"},
		help:      "Set key-value pairs",
		usage:     "set <bucket> <key> <value>",
		validates: NewValidats().NumArgs(3),
		executor:  commandSet,
	})
	r.Register(&dbCommand{
		db:        db,
		alias:     []string{"merge-all-buckets-into-one"},
		help:      "Merges all buckets data into one bucket, keep only one bucket when this command success.",
		usage:     "merge-all-buckets-into-one <bucket-name>",
		validates: NewValidats().NumArgs(1),
		executor:  commandMergeAllBucketIntoOne,
	})
	return r
}

func bytesToString(b []byte) string {
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

type matcher struct {
	re *regexp.Regexp
}

func newMatcher(pattern string) (*matcher, error) {
	if pattern == "" {
		return &matcher{}, nil
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, errors.New("bad pattern synta")
	}
	return &matcher{re: re}, nil
}

func (m *matcher) Match(b []byte) bool {
	if m.re == nil {
		return true
	}
	return m.re.Match(b)
}

type dbCommand struct {
	db        *bolt.DB
	help      string
	alias     []string
	validates Validates
	usage     string
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

func (g *dbCommand) Usage() string {
	if g.usage != "" {
		return g.help + ".\nUsage: " + g.usage
	}
	return g.help
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
			ctx.Printf(bytesToString(v))
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
	var pattern string
	if len(args) == 1 {
		pattern = args[0]
	}
	match, err := newMatcher(pattern)
	if err != nil {
		return err
	}
	err = db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			if !match.Match(name) {
				return nil
			}
			s := b.Stats()
			if !tl.add(ctx, []string{
				bytesToString(name),
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
	var withValue bool
	var pattern string
	switch len(args) {
	case 2:
		if args[1] == "withvalue" {
			withValue = true
		} else {
			pattern = args[1]
		}
	case 3:
		withValue = true
		if args[1] == "withvalue" {
			pattern = args[2]
		} else if args[2] == "withvalue" {
			pattern = args[1]
		}
	default:
	}
	match, err := newMatcher(pattern)
	if err != nil {
		return err
	}
	var tl *tablePrinter
	if withValue {
		tl = newTablePrinter([]string{"key", "value"})
	} else {
		tl = newTablePrinter([]string{"key"})
	}

	err = db.View(func(tx *bolt.Tx) error {
		bu := tx.Bucket(stringToBytes(args[0]))
		if bu == nil {
			ctx.Printf("err: bucket not found")
			return nil
		}
		return bu.ForEach(func(k, v []byte) error {
			if !match.Match(k) {
				return nil
			}
			r := []string{bytesToString(k)}
			if withValue {
				r = append(r, bytesToString(v))
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

func commandCopyBucket(db *bolt.DB, ctx *Context, args []string) error {
	dst, src := args[0], args[1]
	return db.Update(func(tx *bolt.Tx) error {
		s := tx.Bucket(stringToBytes(src))
		if s == nil {
			return nil
		}
		d, err := tx.CreateBucketIfNotExists(stringToBytes(dst))
		if err != nil {
			return err
		}
		return s.ForEach(func(k, v []byte) error {
			return d.Put(k, v)
		})
	})
}

func commandRemove(db *bolt.DB, ctx *Context, args []string) error {
	if args[0] == "key" {
		if len(args) < 3 {
			return fmt.Errorf("remove a key must provides bucket and key")
		}
		bucket := stringToBytes(args[1])
		return db.Update(func(tx *bolt.Tx) error {
			bu := tx.Bucket(bucket)
			if bu == nil {
				return nil
			}
			for _, v := range args[2:] {
				if err := bu.Delete(stringToBytes(v)); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if len(args) < 2 {
		return fmt.Errorf("remove a bucket must provides bucket name")
	}
	return db.Update(func(tx *bolt.Tx) error {
		for _, v := range args[1:] {
			if err := tx.DeleteBucket(stringToBytes(v)); err != nil {
				return err
			}
		}
		return nil
	})
}

func commandSet(db *bolt.DB, ctx *Context, args []string) error {
	return db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(stringToBytes(args[0]))
		if err != nil {
			return err
		}
		return b.Put(stringToBytes(args[1]), stringToBytes(args[2]))
	})
}

func commandMergeAllBucketIntoOne(db *bolt.DB, ctx *Context, args []string) error {
	var buckets [][]byte
	err := db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			buckets = append(buckets, name)
			return nil
		})
	})
	dest := stringToBytes(args[0])
	if err != nil {
		return err
	}
	return db.Update(func(tx *bolt.Tx) error {
		d, err := tx.CreateBucketIfNotExists(dest)
		if err != nil {
			return err
		}
		for _, b := range buckets {
			if bytes.Equal(dest, b) {
				continue
			}
			bu := tx.Bucket(b)
			if bu == nil {
				continue
			}
			err := bu.ForEach(func(k, v []byte) error {
				return d.Put(k, v)
			})
			if err != nil {
				return err
			}
			if err := tx.DeleteBucket(b); err != nil {
				return err
			}
		}
		return nil
	})
}
