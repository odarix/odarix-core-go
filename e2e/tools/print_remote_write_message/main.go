package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/prompb"
)

func main() {
	flag.Parse()
	input := flag.Arg(0)
	if input == "" {
		fmt.Println("Path to input file is required")
		os.Exit(2)
	}
	data, err := ioutil.ReadFile(input)
	if err != nil {
		log.Printf("fail to read %s: %s", input, err)
		os.Exit(1)
	}
	var msg prompb.WriteRequest
	if err := proto.Unmarshal(data, &msg); err != nil {
		log.Printf("file to unmarshal message: %s", err)
		os.Exit(1)
	}
	for _, ts := range msg.Timeseries {
		fmt.Print("{")
		for i, label := range ts.Labels {
			if i > 0 {
				fmt.Print(", ")
			}
			fmt.Printf("%s=%q", label.Name, label.Value)
		}
		fmt.Print("} ")
		for i, sample := range ts.Samples {
			if i > 0 {
				fmt.Print(" ")
			}
			fmt.Printf("(%d, %f)", sample.Timestamp, sample.Value)
		}
		fmt.Print("\n")
	}
}
