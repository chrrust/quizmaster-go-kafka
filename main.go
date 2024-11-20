package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func ReadConfig() kafka.ConfigMap {
	// reads the client configuration from client.properties
	// and returns it as a key-value map
	m := make(map[string]kafka.ConfigValue)

	file, err := os.Open("client.properties")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open file: %s", err)
		os.Exit(1)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "#") && len(line) != 0 {
			kv := strings.Split(line, "=")
			parameter := strings.TrimSpace(kv[0])
			value := strings.TrimSpace(kv[1])
			m[parameter] = value
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Failed to read file: %s", err)
		os.Exit(1)
	}

	return m
}

func createProducer(topic string, config kafka.ConfigMap) *kafka.Producer {
	// creates a new producer instance
	p, _ := kafka.NewProducer(&config)

	// go-routine to handle message delivery reports and
	// possibly other event types (errors, stats, etc)
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Produced event to topic %s: key = %-10s value = %s\n",
						*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
				}
			}
		}
	}()

	return p
}

func createConsumer(topic string, config kafka.ConfigMap) *kafka.Consumer {
	// sets the consumer group ID and offset
	config["group.id"] = "go-group-1"
	config["auto.offset.reset"] = "earliest"

	// creates a new consumer instance
	c, _ := kafka.NewConsumer(&config)

	// subscribes to the topic
	c.Subscribe(topic, nil)

	return c
}

// func consume(topic string, config kafka.ConfigMap) {

// 	// creates a new consumer and subscribes to your topic
// 	consumer, _ := kafka.NewConsumer(&config)
// 	consumer.SubscribeTopics([]string{topic}, nil)

// 	run := true
// 	for run {
// 		// consumes messages from the subscribed topic and prints them to the console
// 		e := consumer.Poll(1000)
// 		switch ev := e.(type) {
// 		case *kafka.Message:
// 			// application-specific processing
// 			fmt.Printf("Consumed event from topic %s: key = %-10s value = %s\n",
// 				*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
// 		case kafka.Error:
// 			fmt.Fprintf(os.Stderr, "%% Error: %v\n", ev)
// 			run = false
// 		}
// 	}

// 	// closes the consumer connection
// 	consumer.Close()
// }

func main() {
	topic := "leesha_game"
	config := ReadConfig()

	producer := createProducer(topic, config)
	// consumer := createConsumer(topic, config)

	// go func() {
	// 	for {
	// 		event := consumer.Poll(1000)
	// 	}
	// }

	questions := makeQuestions()
	for _, question := range questions {
		questionJson, err := json.Marshal(question)
		if err != nil {
			fmt.Printf("Failed to marshal question: %v\n", err)
			os.Exit(1)
		}

		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte("key"),
			Value:          questionJson,
		}, nil)

		go func() {

		}()
	}

	producer.Flush(15 * 1000)
	producer.Close()
}

func parseAnswer(answerJson string) Answer {
	// parses the JSON string into a Question struct
	var ans Answer
	_ = json.Unmarshal([]byte(answerJson), &ans)
	return ans
}

type Question struct {
	EventName     string `json:"@event_name"`
	Dokumentasjon string `json:"dokumentasjon"`
	SpørsmålId    string `json:"spørsmålId"`
	Kategori      string `json:"kategori"`
	Spørsmål      string `json:"spørsmål"`
	Svarformat    string `json:"svarformat"`
}

type Answer struct {
	Type       string `json:"type"`
	SvarId     string `json:"svarId"`
	SpørsmålId string `json:"spørsmålId"`
	Kategori   string `json:"kategori"`
	Lagnavn    string `json:"lagnavn"`
	Svar       string `json:"svar"`
	Opprettet  string `json:"opprettet"`
}

func makeQuestions() []Question {
	return []Question{
		{
			EventName:     "SPØRSMÅL",
			Dokumentasjon: "tull",
			SpørsmålId:    "96acb8f4-4b1e-4f0a-9d54-fcdda1223d9d",
			Kategori:      "aritmetikk",
			Spørsmål:      "49 - 62",
			Svarformat:    "Svaret må rundes",
		},
	}
}
