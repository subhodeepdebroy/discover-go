package main

import (
	"github.com/gin-gonic/gin"
	"log"
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	//"go.mongodb.org/mongo-driver/mongo/readpref"
)

const uri = "mongodb://appbazaar:juWj8zZyxP2v@10.100.3.216:27017,10.100.4.216:27017,10.100.4.217:27017/appbazaar?replicaSet=rs1&readPreference=secondaryPreferred"
var dbClient *mongo.Client

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

func main() {
	connectMongo();	
	router := gin.Default()
	router.GET("/popularkeywords", getKeywords)
	router.Run()
}

func getKeywords(ctx *gin.Context) {
	var result bson.M
	filter := bson.D{{}}; 
	coll := dbClient.Database("appbazaar").Collection("discover_keywordscollection");
	err := coll.FindOne(context.TODO(), filter).Decode(&result);

	if err != nil {
		if err == mongo.ErrNoDocuments {
			return
		}
		log.Fatal(err);
	}
	fmt.Printf("found document %v", result);
	ctx.JSON(200, gin.H{
		"res": result,
	});
}