package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
)

func main() {
	buf := make([]byte, 10)
	io.ReadFull(rand.Reader, buf)
	id := hex.EncodeToString(buf)
	fmt.Println(id)
	fmt.Println(string(buf))
}
