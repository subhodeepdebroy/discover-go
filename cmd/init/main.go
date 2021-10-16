package main

import (
	"github.com/gin-gonic/gin"
	"log"
	"context"
	"fmt"
	"time"
	"encoding/json"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"github.com/go-redis/redis/v8"
)

const uri = "mongodb://appbazaar:juWj8zZyxP2v@10.100.3.216:27017,10.100.4.216:27017,10.100.4.217:27017/appbazaar?replicaSet=rs1&readPreference=secondaryPreferred"
var dbClient *mongo.Client;
var localRedis *redis.Client;
var globalRedis *redis.Client;
	
func rClient(url string) *redis.Client {	
client := redis.NewClient(&redis.Options{
	Addr: url,
})

return client
}

func connectRedis(){
	localRedis = rClient("localhost:6999")
	globalRedis = rClient("s-ab-r-0001-001.aelcbq.0001.aps1.cache.amazonaws.com:6379")
}

func connectMongo(){
	// Create a new client and connect to the server
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(uri));
	fmt.Printf("%v", client);
	if err != nil {	
		panic(err)
	}
	fmt.Println("Successfully connected and pinged.")
	dbClient = client;
}

func getKeywords(ctx *gin.Context) {
	result := getData()
	ctx.JSON(200, gin.H{
		"res": result,
	});
}

func getFromMongo() interface{} {
	var result interface{}
	filter := bson.D{{}}; 
	projection := bson.D{{"_id", 0}};
	opts := options.FindOne().SetProjection(projection);
	coll := dbClient.Database("appbazaar").Collection("discover_keywordscollection");
	err := coll.FindOne(context.TODO(), filter, opts).Decode(&result);

	if err != nil {
		log.Fatal(err);
	}
	fmt.Printf("found document %v", result);
	return result;
}

func getData() interface{} {
	var data interface{};
	var err error;
	data, err = GetValue("DISCOVER:popularkeywords:go", localRedis);
	if err != nil {
		data, err = GetValue("DISCOVER:popularkeywords:go", globalRedis);
		if err != nil {
			data = getFromMongo();
			SetValueWithTTL("DISCOVER:popularkeywords:go", data, 300, localRedis);
			SetValueWithTTL("DISCOVER:popularkeywords:go", data, 300, globalRedis)
		}
		SetValueWithTTL("DISCOVER:popularkeywords:go", data, 300, localRedis)
	}
	return data;
}

func SetValueWithTTL(key string, value interface{}, ttl int, redisClient *redis.Client) (bool, error) {
   serializedValue, _ := json.Marshal(value)
   err := redisClient.Set(context.TODO(), key, serializedValue, time.Duration(ttl)*time.Second).Err()
   return true, err
}

func GetValue(key string, redisClient *redis.Client) (string, error) {
	var deserializedValue interface{}
   serializedValue, err := redisClient.Get(context.TODO(), key).Result()
   json.Unmarshal([]byte(serializedValue), &deserializedValue)
   return serializedValue, err
}

func main() {
	connectMongo();	
	connectRedis()
	router := gin.Default()
	router.GET("/popularkeywords", getKeywords)
	router.Run()
}