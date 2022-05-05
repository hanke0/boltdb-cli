package command

import (
	"fmt"
	"os"
	"strings"
)

type Command interface {
	Alias() []string
	Help() string
	Execute(ctx *Context, args []string) error
	Check(ctx *Context, args []string) error
}

type Usage interface {
	Usage() string
}

type Commands interface {
	Execute(ctx *Context, cmd string) error
}

type Register interface {
	Commands
	Register(Command)
}

func NewRegister() Register {
	r := &commands{}
	r.Register(&helpCommand{cmd: r})
	r.Register(exitCommand{})
	return r
}

type commands struct {
	cmds []Command
}

func (c *commands) find(cmd string) Command {
	for _, cc := range c.cmds {
		for _, n := range cc.Alias() {
			if n == cmd {
				return cc
			}
		}
	}
	return nil
}

func (c *commands) findE(ctx *Context, cmd string) Command {
	e := c.find(cmd)
	if e == nil {
		ctx.Printf("unknown command: '%s', press ?/h for help\n", cmd)
		return nil
	}
	return e
}

func (c *commands) Execute(ctx *Context, cmd string) error {
	fullargs := strings.Fields(cmd)
	e := c.findE(ctx, fullargs[0])
	if e == nil {
		return nil
	}
	var args []string
	if len(fullargs) > 1 {
		args = fullargs[1:]
	}
	if err := e.Check(ctx, args); err != nil {
		ctx.Printf("error: %v\n", err)
		return nil
	}
	if err := e.Execute(ctx, args); err != nil {
		ctx.Printf("error: %v\n", err)
		return nil
	}
	return nil
}

func (c *commands) Register(f Command) {
	c.cmds = append(c.cmds, f)
}

type helpCommand struct {
	cmd *commands
}

func (c *helpCommand) Alias() []string {
	return []string{"help", "h", "?"}
}

func (c *helpCommand) Check(ctx *Context, args []string) error {
	var v Validates
	return v.MaxArgs(1).Finish()(ctx, args)
}

func (c *helpCommand) Execute(ctx *Context, args []string) error {
	if len(args) > 0 {
		e := c.cmd.findE(ctx, args[0])
		if e != nil {
			if v, ok := e.(Usage); ok {
				ctx.Printf(v.Usage() + "\n")
			} else {
				fmt.Fprintf(ctx.Output(), "%s    %s\n", strings.Join(e.Alias(), ", "), e.Help())
			}
		}
		return nil
	}
	for _, v := range c.cmd.cmds {
		fmt.Fprintf(ctx.Output(), "%-24s %s\n", strings.Join(v.Alias(), ", "), v.Help())
	}
	return nil
}

func (c *helpCommand) Help() string {
	return "Print command help text. Specific a command name for more information about it."
}

type exitCommand struct{}

func (exitCommand) Alias() []string { return []string{"exit", "q"} }
func (exitCommand) Help() string    { return "Exit." }
func (exitCommand) Check(ctx *Context, args []string) error {
	return NewValidats().NumArgs(0).Finish()(ctx, args)
}
func (exitCommand) Execute(ctx *Context, args []string) error {
	ctx.Close()
	os.Exit(0)
	return nil
}
