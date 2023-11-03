package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	topic := "HVSE"
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "foo_data",
		"auto.offset.reset": "smallest",
	})

	if err != nil {
		log.Fatal(err)
	}

	if err = consumer.Subscribe(topic, nil); err != nil {
		log.Fatal(err)
	}

	for {
		ev := consumer.Poll(100)
		switch e := ev.(type) {
		case *kafka.Message:
			fmt.Printf("processing order: %s\n", string(e.Value))
		case *kafka.Error:
			fmt.Printf("%+v\n", e)
		}
	}
}
