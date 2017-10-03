package gohtools

import (
	"log"
	"runtime/debug"
)

func CheckFatal(e error) error {
	if e != nil {
		debug.PrintStack()
		log.Println("CHECK: ERROR:", e)
		panic(e)
	}
	return e
}

func CheckNotFatal(e error) error {
	if e != nil {
		debug.PrintStack()
		log.Println("CHECK: ERROR:", e, e.Error())
	}
	return e
}
