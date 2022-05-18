package command

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/peterh/liner"
)

var (
	HistoryFile string
	MaxLines    = 65536
	Prompt      = ">> "
)

func init() {
	home, err := os.UserHomeDir()
	if err != nil {
		home = os.TempDir()
	}
	HistoryFile = filepath.Join(home, ".boltdb-cli.history")
}

type Context struct {
	line    *liner.State
	history string
	prompt  string
	err     error
	output  io.Writer

	command string
}

func NewContext() *Context {
	c := &Context{
		line:    liner.NewLiner(),
		history: HistoryFile,
		prompt:  Prompt,
		output:  os.Stdout,
	}
	c.line.SetCtrlCAborts(true)
	return c
}

func (ctx *Context) Close() error {
	return ctx.line.Close()
}

func (ctx *Context) Command() string {
	return ctx.command
}

func (ctx *Context) Next() bool {
	for {
		line, err := ctx.line.Prompt(ctx.prompt)
		if err == liner.ErrPromptAborted { // nolint:errorlint
			continue
		}
		if line == "" {
			continue
		}
		if err != nil {
			ctx.err = err
			return false
		}
		ctx.command = line
		return true
	}
}

func (ctx *Context) Do(c Commands) {
	if err := c.Execute(ctx, ctx.Command()); err != nil {
		ctx.Fatalf("error: %v\n", err)
	}
	ctx.line.AppendHistory(ctx.Command())
}

func (ctx *Context) ReadLine(prompt string) (string, error) {
	return ctx.line.Prompt(prompt)
}

func (ctx *Context) Err() error {
	return ctx.err
}

func (ctx *Context) SetHistory(f string) {
	ctx.history = f
}

func (ctx *Context) SetPrompt(f string) {
	ctx.prompt = f
}

func (ctx *Context) Output() io.Writer {
	return ctx.output
}

func (ctx *Context) SetOutput(w io.Writer) {
	ctx.output = w
}

func (ctx *Context) ReadHistory() (int, error) {
	f, err := os.Open(ctx.history)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return ctx.line.ReadHistory(f)
}

func (ctx *Context) WriteHistory() (int, error) {
	f, err := os.OpenFile(ctx.history, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0640)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	n, err := ctx.line.WriteHistory(f)
	if err != nil {
		return n, err
	}
	err = keepMaxLine(f, int64(MaxLines))
	return n, err
}

func keepMaxLine(f *os.File, maxLine int64) error {
	if _, err := f.Seek(0, 0); err != nil {
		return err
	}
	stat, err := f.Stat()
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(f.Name()), filepath.Base(f.Name())+".*")
	if err != nil {
		return err
	}
	if err := tmp.Chmod(stat.Mode()); err != nil {
		return err
	}
	var lines []string
	scan := bufio.NewScanner(f)
	for scan.Scan() {
		t := scan.Text()
		var pos = -1
		for i := range lines {
			if lines[i] == t {
				pos = i
				break
			}
		}
		if pos > 0 {
			lines[len(lines)-1], lines[pos] = lines[pos], lines[len(lines)-1]
		} else {
			lines = append(lines, t)
		}
	}

	lineCount := int64(len(lines))
	for _, line := range lines {
		if lineCount > maxLine {
			lineCount--
			continue
		}
		if _, err := tmp.WriteString(line); err != nil {
			return err
		}
		if _, err := tmp.Write([]byte{'\n'}); err != nil {
			return err
		}
	}
	f.Close()
	tmp.Close()
	return os.Rename(tmp.Name(), f.Name())
}

func (ctx *Context) Printf(format string, args ...interface{}) {
	if format[len(format)-1] != '\n' {
		format += "\n"
	}
	fmt.Fprintf(ctx.Output(), format, args...)
}

func (ctx *Context) Fatalf(format string, args ...interface{}) {
	ctx.Printf(format, args...)
	ctx.Close()
	os.Exit(1)
}
