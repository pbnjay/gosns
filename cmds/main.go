package main

import (
	"github.com/pbnjay/gosns"
	"log"
	"os"
)

func JustPrint(msg *gosns.Message) {
	if msg == nil {
		log.Println("Topic Subscription Confirmed.")
		return
	}
	log.Println("-----")
	log.Printf("timestamp:  %v\n", msg.Timestamp)
	log.Printf("message-id: %s\n", msg.MessageId)
	log.Printf("subject:    '%s'\n\n", msg.Subject)
	log.Println(msg.Message)
	log.Println("-----")
}

func main() {
	if len(os.Args) != 3 {
		log.SetFlags(0)
		log.Fatalf("USAGE: %s topic:arn /web/endpoint", os.Args[0])
	}
	snsServer := &gosns.Server{}
	snsServer.Logger = log.New(os.Stderr, "GOSNS ", log.LstdFlags)
	snsServer.AddTopic(os.Args[1], os.Args[2], JustPrint)
	log.Fatal(snsServer.ListenAndServe(":8080"))
}
