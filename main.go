package main

import (
	"encoding/json"
	"fmt"
	"os"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main(){
	consumeFromKafka()
}

func SendToKafka(){
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "209.126.10.224:9092"})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer p.Close()

	// Delivery report handler for produced messages.
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	jsonRequest:=`{"name":"kiptoo"}`
	b,bEr:=json.Marshal(jsonRequest)
	if bEr != nil {
		fmt.Println(bEr)
	}

	// Produce messages to topic (asynchronously)
	topic := "WelcomeKafka"

	er:=p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          b,
	}, nil)

	if er != nil {
		fmt.Println(er)
	}

	// Wait for message deliveries before shutting down
	p.Flush(15 * 1000)
}
func consumeFromKafka(){
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "209.126.10.224:9092",
		"group.id":          "NamesGroup",
		"auto.offset.reset": "earliest",
	})
	defer c.Close()

	if err != nil {
		fmt.Println(err)
	}

	subscribeError:=c.SubscribeTopics([]string{"WelcomeKafka"}, nil)
	if subscribeError != nil {
		fmt.Println(subscribeError)
	}

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
}