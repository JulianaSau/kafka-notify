package main
import {
	"encoding/json",
	"errors",
	"fmt",
	"log",
	"net/http",
	"strconv",

    "kafka-notify/pkg/models",

	"github.com/IBM/sarama",
	"github.com/gin-gonic/gin",
}

const (
	ProducerPort = ":8080",
	KafkaServerAddress = "localhost:9092",
	KafkaTopic = "notifications",
)

// HELPER FUNCTIONS

var ErrUserNotFoundInProducer = errors.New("User not found")

func findUserByID(id int, users []models.User) (models.User, error) {
	for _, user := range users {
		if user.ID == id {
			return user, nil
		}
	}

	return models.User{}, ErrUserNotFoundInProducer
}

func getIDFromRequest(formValue string, ctx *gin.Context) (int, error) {
	id, err := strconv.Atoi(ctx.PostForm(formValue))
	if err != nil {
		return 0, fmt.Errorf(
			"Failed to parse ID from form values %s: %w", formValue, err		
		)
	}

	return id, nil
}

// KAFKA RELATED FUNCTIONS

func sendKafkaMessage(producer, sarama.SyncProducer,
users []models.User, ctx *gin.Context, fromID, toID int) error {
	//  retrieving the message from the context and then attempts
	//  to find both the sender and the recipient using their IDs.
	message := ctx.PostForm("message")

	fromUser, err := findUserByID(fromId, users)
	if err != nil {
		return fmt.Errorf("Failed to find from user: %w", err)
	}

	toUser, err := findUserByID(toID, users)
	if err != nil {
		return fmt.Errorf("Failed to find to user: %w", err)
	}

	// Initializes a Notification struct that encapsulates 
	// information about the sender, the recipient, and the 
	// actual message.
	notification := models.Notification{
		From: fromUser,
		To: toUser, 
		Message: message,
	}

	NotificationJSON, err := json.Marshal(notification)
	if err != nil {
		return fmt.Errorf("Failed to marshal notification: %w", err)
	}

	// Constructs a ProducerMessage for the "notifications" topic, 
	// setting the recipient’s ID as the Key and the message content, 
	// which is the serialized form of the Notification as the Value
	msg := &sarama.ProducerMessage{
		Topic: KafkaTopic,
		Key: sarama.StringEncoder(strconv.Itoa(toUser.ID)),
		Value: sarama.StringEncoder(notificationJSON)
	}

	// Sends the constructed message to the "notifications" topic.
	_, _, err = producer.SendMessage(msg)
	return err
}

// function serves as an endpoint handler for the /send POST request. 
// It processes the incoming request to ensure valid sender and
//  recipient IDs are provided

// Depending on the result, it dispatches appropriate HTTP responses: 
// a 404 Not Found for nonexistent users,
//  a 400 Bad Request for invalid IDs, 
// and a 500 Internal Server Error for other failures, 
// along with a specific error message.
func SendMessageHandler(producer sarama.SyncProducer,
	users []models.User) gin.HandlerFunc {
	return func(ctx *gin.Context){
		// get from User ID
		fromID, err := getIDFromRequest("fromID", ctx)
		// throw error if not found
		if err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{"message": err.Error()})
			return
		}

		// get to User ID
		toID, err := getIDFromRequest("toID", ctx)
		// throw error if not found
		if err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{"message": err.Error()})
			return
		}

		// send message to Kafka
		err = sendKafkaMessage(producer, users, ctx, fromID, toID)
		// throw error if not found
		if errors.Is(err, ErrUserNotFoundInProducer) {
			ctx.JSON(http.StatusNotFound, gin.H{"message":"User not found"})
			return
		}

		// throw error if failed to send message to Kafka
		if err != nil {
			ctx.JSON(http.StatusInternalServerError, gin.H{
				"message": err.Error(),
			})
			return
		}

		// otherwise send success message
		ctx.JSON(http.StatusOK, gin.H{
			"message": "Notification sent successfully"
		})
	}
}

func setupProducer() (sarama.SyncProducer, error) {
	// Initializes a new default configuration for Kafka. 
	// Think of it as setting up the parameters before connecting to the broker.
	config := sarama.NewConfig()
	// Ensures that the producer receives an acknowledgment once the message is successfully stored in the "notifications" topic
	config.Producer.Return.Success = true
	// Initializes a synchronous Kafka producer that connects to the Kafka broker running at localhost:9092
	producer, err := sarama.NewSyncProducer([]string{KafkaServerAddress}, config)

	if err != nil {
		return nil, fmt.Errorf("failed to setup producer: %w", err)
	}
	return producer, nil
}

func main() {
	users := []models.User{
		{ID: 1, Name: "Emma"},
		{ID: 2, Name: "Bruno"},
		{ID: 3, Name: "Rick"},
		{ID: 4, Name: "Lena"},
	}

	// initialize a Kafka producer via the setupProducer() function.
	producer, err := setupProducer()
	if err != nil {
		log.Fatalf("Failed to initialise producer %v", err)
	}

	defer producer.Close()

	gin.SetMode(gin.ReleaseMode)
	// create a Gin router via gin.Default(), setting up a web server.
	router := gin.Default()
	// Next, you define a POST endpoint /send to handle notifications. 
	// This endpoint expects the sender and recipient’s IDs and the message content.
	// The notification is processed upon receiving a POST request via 
	// the sendMessageHandler() function, and an appropriate HTTP response is dispatched.
	router.POST("/send", SendMessageHandler(producer, users))

	fmt.Printf("Kafka PRODUCER 📨 started at http://localhost%s\n", 
		ProducerPort)

	if err := router.Run(ProducerPort); err != nil {
		log.Printf("failed to run the server: %v", err)
	}
}