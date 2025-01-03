package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"

	"kafka-notify/pkg/models"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
)

const (
	ConsumerGroup      = "notifications-group"
	ConsumerTopic      = "notifications"
	ConsumerPort       = ":8081"
	KafkaServerAddress = "localhost:9092"
)

// helper functions

var ErrNoMessagesFound = errors.New("no messages found")

func getUserIDFromRequest(ctx *gin.Context) (string, error) {
	userID := ctx.Param("userID")
	if userID == "" {
		return "", ErrNoMessagesFound
	}

	return userID, sarama.ErrCannotTransitionNilError
}

// Notification Storage

type UserNotifications map[string][]models.Notification

type NotificationStore struct {
	data UserNotifications
	mu   sync.RWMutex
}

func (ns *NotificationStore) Add(userID string,
	notification models.Notification) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.data[userID] = append(ns.data[userID], notification)
}

func (ns *NotificationStore) Get(userID string) []models.Notification {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	return ns.data[userID]
}

// kafka related functions
// The Consumer struct has a store field, which is a reference
// to the NotificationStore to keep track of the received notifications.
type Consumer struct {
	store *NotificationStore
}

// Setup() and Cleanup() methods are required to satisfy the
// sarama.ConsumerGroupHandler interface. While they will NOT
//
//	be used in this tutorial, they can serve potential roles
//
// for initialization and cleanup during message consumption but
// act as placeholders here.
func (*Consumer) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (*Consumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }

// The consumer listens for new messages on the topic.
//
//	For each message, it fetches the userID (the Key of the message),
//	un-marshals the message into a Notification struct, and adds the
//	notification to the NotificationStore.
func (consumer *Consumer) ConsumeClaim(
	session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		userID := string(msg.Key)
		var notification models.Notification
		err := json.Unmarshal(msg.Value, &notification)

		if err != nil {
			log.Printf("Failed to unmarshal notification: %v", err)
			continue
		}

		consumer.store.Add(userID, notification)
		session.MarkMessage(msg, "")

	}
	return nil
}

func initializeConsumerGroup() (sarama.ConsumerGroup, error) {
	// Initializes a new default configuration for Kafka.
	config := sarama.NewConfig()

	// Creates a new Kafka consumer group that connects to
	// the broker running on localhost:9092.
	// The group name is "notifications-group"
	consumerGroup, err := sarama.NewConsumerGroup(
		[]string{KafkaServerAddress}, ConsumerGroup, config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialise consumer group: %w", err)
	}

	return consumerGroup, nil
}

// sets up the Kafka consumer group, listens for incoming messages,
// and processes them using the Consumer struct methods.
func setupConsumerGroup(ctx context.Context, store *NotificationStore) {
	consumerGroup, err := initializeConsumerGroup()

	if err != nil {
		log.Fatalf("Failed to initialize consumer group: %v", err)
	}

	defer consumerGroup.Close()

	consumer := &Consumer{
		store: store,
	}

	// It runs a for loop indefinitely, consuming messages from the
	// “notifications” topic and processing any errors that arise.
	for {
		err := consumerGroup.Consume(ctx, []string{ConsumerTopic}, consumer)
		if err != nil {
			log.Printf("Error from consumer: %v", err)
		}
		if ctx.Err() != nil {
			return
		}
	}
}

func handleNotifications(ctx *gin.Context, store *NotificationStore) {
	// Initially, it attempts to retrieve the userID from the request.
	// If it doesn’t exist, it returns a 404 Not Found status.
	userID, err := getUserIDFromRequest(ctx)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"message": err.Error()})
		return
	}

	// fetches the notifications for the provided user ID from the
	// NotificationStore. Depending on whether the user has notifications,
	// it responds with a 200 OK status and either an empty notifications
	// slice or sends back the current notifications.
	notifications := store.Get(userID)
	if len(notifications) == 0 {
		ctx.JSON(http.StatusOK, gin.H{
			"message":       "No notifications found for user",
			"notifications": []models.Notification{},
		})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"notifications": notifications})
}

func main() {
	// Creates an instance of NotificationStore to hold the notifications
	store := &NotificationStore{data: make(UserNotifications)}

	// Sets up a cancellable context that can be used to stop the consumer group.
	ctx, cancel := context.WithCancel(context.Background())
	// Starts the consumer group in a separate Goroutine,
	// allowing it to operate concurrently without blocking the main thread.
	go setupConsumerGroup(ctx, store)
	defer cancel()

	gin.SetMode(gin.ReleaseMode)
	// create a Gin router and define a GET endpoint
	// /notifications/:userID that will fetch the notifications for a
	// specific user via the handleNotifications() function when accessed.
	router := gin.Default()
	router.GET("/notifications/:userID", func(ctx *gin.Context) {
		handleNotifications(ctx, store)
	})

	fmt.Printf("Kafka CONSUMER (Group: %s) 👥📥 "+
		"started at http://localhost%s\n", ConsumerGroup, ConsumerPort)

	if err := router.Run(ConsumerPort); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
