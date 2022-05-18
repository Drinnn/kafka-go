package main

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	producer := NewKafkaProducer()
	Publish("Mensagem", "teste", nil, producer)
	producer.Flush(1000)
}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "kafka_go_kafka:9092",
	}
	producer, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Println(err.Error())
	}

	return producer
}

func Publish(msg string, topic string, key []byte, producer *kafka.Producer) error {
	formattedMsg := &kafka.Message{
		Value:          []byte(msg),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
	}

	err := producer.Produce(formattedMsg, nil)
	if err != nil {
		return err
	}

	return nil
}
