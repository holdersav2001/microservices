package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type TradeEvent struct {
	ProcessID   string `json:"process_id"`
	TradeID     int    `json:"trade_id"`
	Status      string `json:"status"`
	Timestamp   string `json:"timestamp"`
	Application string `json:"application"`
}

type EventData struct {
	EventName       string            `json:"eventName"`
	TradeID         int               `json:"tradeId"`
	EventStatus     string            `json:"eventStatus"`
	BusinessDate    string            `json:"businessDate"`
	EventTime       string            `json:"eventTime"`
	EventType       string            `json:"eventType"`
	BatchOrRealtime string            `json:"batchOrRealtime"`
	Resource        string            `json:"resource"`
	Message         string            `json:"message"`
	Details         map[string]string `json:"details"`
}

func validateMessage(msg *TradeEvent) bool {
	if msg.TradeID == 0 || msg.Status == "" || msg.Timestamp == "" || msg.ProcessID == "" || msg.Application == "" {
		log.Printf("Invalid message: %+v", msg)
		return false
	}
	return true
}

func transformMessage(msg *TradeEvent) (*EventData, error) {
	eventTime, err := time.Parse(time.RFC3339, msg.Timestamp)
	if err != nil {
		return nil, fmt.Errorf("failed to parse timestamp: %w", err)
	}

	return &EventData{
		EventName:       fmt.Sprintf("%s-%d", msg.ProcessID, msg.TradeID),
		TradeID:         msg.TradeID,
		EventStatus:     msg.Status,
		BusinessDate:    eventTime.Format("2006-01-02"),
		EventTime:       eventTime.Format(time.RFC3339),
		EventType:       "MESSAGE",
		BatchOrRealtime: "Realtime",
		Resource:        "trading app",
		Message:         "",
		Details: map[string]string{
			"messageId":    fmt.Sprintf("%d", msg.TradeID),
			"messageQueue": "trade_events",
			"application":  msg.Application,
		},
	}, nil
}

func processMessage(msg *kafka.Message) error {
	var tradeEvent TradeEvent
	err := json.Unmarshal(msg.Value, &tradeEvent)
	if err != nil {
		return fmt.Errorf("failed to unmarshal message: %w", err)
	}

	if !validateMessage(&tradeEvent) {
		return fmt.Errorf("invalid message format")
	}

	transformedEvent, err := transformMessage(&tradeEvent)
	if err != nil {
		return fmt.Errorf("failed to transform message: %w", err)
	}

	log.Printf("Processed event: %+v", transformedEvent)
	return nil
}

func main() {
	config := kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "go-consumer-group",
		"auto.offset.reset": "earliest",
	}

	consumer, err := kafka.NewConsumer(&config)
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %s", err)
	}
	defer consumer.Close()

	topic := "trade_events"
	err = consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.Fatalf("Failed to subscribe to topic %s: %s", topic, err)
	}

	log.Printf("Listening for messages on topic %s...", topic)

	for {
		ev := consumer.Poll(100)
		if ev == nil {
			continue
		}

		switch e := ev.(type) {
		case *kafka.Message:
			err := processMessage(e)
			if err != nil {
				log.Printf("Error processing message: %s", err)
			}
		case kafka.Error:
			log.Printf("Kafka error: %s", e)
		}
	}
}
