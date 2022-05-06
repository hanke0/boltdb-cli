package command

import (
	"fmt"
)

type ValidateFunc func(ctx *Context, args []string) error

type Validates []ValidateFunc

func NewValidats() Validates {
	return nil
}

func (v Validates) Finish() ValidateFunc {
	return func(ctx *Context, args []string) error {
		for _, c := range v {
			if err := c(ctx, args); err != nil {
				return err
			}
		}
		return nil
	}
}

func (v Validates) Append(f ValidateFunc) Validates {
	v = append(v, f)
	return v
}

func (v Validates) NumArgs(n int) Validates {
	return v.Append(func(ctx *Context, args []string) error {
		if len(args) != n {
			return fmt.Errorf("expect %d arguments, got %d", n, len(args))
		}
		return nil
	})
}

func (v Validates) MaxArgs(n int) Validates {
	return v.Append(func(ctx *Context, args []string) error {
		if len(args) > n {
			return fmt.Errorf("expect max %d arguments, got %d", n, len(args))
		}
		return nil
	})
}

func (v Validates) MinArgs(n int) Validates {
	return v.Append(func(ctx *Context, args []string) error {
		if len(args) < n {
			return fmt.Errorf("expect minimum %d arguments, got %d", n, len(args))
		}
		return nil
	})
}

func (v Validates) NumArgsChoice(n ...int) Validates {
	return v.Append(func(ctx *Context, args []string) error {
		for _, v := range n {
			if v == len(args) {
				return nil
			}
		}
		return fmt.Errorf("expect %+v arguments, got %d", n, len(args))
	})
}

func (v Validates) Choices(pos int, choices []string) Validates {
	return v.Append(func(ctx *Context, args []string) error {
		if len(args) <= pos {
			return nil
		}
		for _, c := range choices {
			if c == args[pos] {
				return nil
			}
		}
		return fmt.Errorf("%d-th arguments should be one of %+v", pos+1, choices)
	})
}
