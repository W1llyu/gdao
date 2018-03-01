package utils

import (
	"fmt"
	"log"
)

func WarnOnError(err error, msg string) {
	if err != nil {
		log.Printf("%s: %s", msg, err)
	}
}

func Fatal(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func Info(msg string) {
	log.Printf("[INFO] %s", msg)
}

func Infof(format string, a ...interface{}) {
	log.Printf("[INFO] %s", fmt.Sprintf(format, a...))
}

func Error(err error, msg string) {
	log.Printf("[ERROR] %s %s", msg, err)
}
