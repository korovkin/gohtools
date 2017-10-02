package gohtools

import (
	"strings"
)

func KeepShort(s *string, maxLenght int) *string {
	if s == nil {
		NIL := "nil"
		ss := KeepShort(&NIL, maxLenght)
		return ss
	}
	if len(*s) > maxLenght {
		ss := (*s)[0:maxLenght] + " ..."
		return &ss
	}
	return s
}

func Implode(sep, a, b string) string {
	return a + sep + b
}

func Explode(sep, x string) (a, b string) {
	l := strings.Split(x, sep)
	if len(l) > 1 {
		return l[0], l[1]
	} else if len(l) > 0 {
		return l[0], ""
	}
	return "", ""
}
