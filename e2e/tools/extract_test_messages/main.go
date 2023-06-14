package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/pierrec/lz4/v4"
)

func main() {
	n := flag.Int("n", 10, "Number of messages")
	output := flag.String("o", "test_data", "Output directory")
	flag.Parse()
	input := flag.Arg(0)
	if input == "" {
		fmt.Println("Path to input file required")
		os.Exit(2)
	}

	f, err := os.Open(input)
	if err != nil {
		log.Printf("fail to open %s: %s", input, err)
		os.Exit(1)
	}
	defer func() {
		_ = f.Close()
	}()
	z := lz4.NewReader(f)

	for i := 0; i < *n; i++ {
		log.Printf("read message #%d", i)
		var size uint32
		err := binary.Read(z, binary.LittleEndian, &size)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			log.Printf("fail to read message size: %s", err)
			os.Exit(1)
		}
		destPath := filepath.Join(*output, fmt.Sprintf("remote_write_%03d.protobuf.bin", i))
		dest, err := os.Create(destPath)
		if err != nil {
			log.Printf("fail to create destination file: %s", err)
			os.Exit(1)
		}
		if _, err := io.CopyN(dest, z, int64(size)); err != nil {
			log.Printf("fail to copy message: %s", err)
		}
		_ = dest.Close()
		if err != nil {
			os.Exit(1)
		}
	}
	log.Print("Success!")
}
