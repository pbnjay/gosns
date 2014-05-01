// Package gosns implements a dead-simple Amazon SNS HTTP notification endpoint
// server. It allows you to subscribe and respond to notifications with minimal
// boilerplate code using a simple API.
package gosns

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

const amzTimeFormat = "2006-01-02T15:04:05.999999999Z"

type Server struct {
	Logger *log.Logger
	topics map[string]*topicDescription
}

type topicDescription struct {
	TopicARN string
	Callback func(*Message)
}

type Message struct {
	Subject   string
	Message   string
	MessageId string
	Timestamp time.Time
}

// AddTopic adds an http endpoint for the specified topicARN which will
// automatically handle SNS subscription confirmation, and parse message
// notifications which are sent to the goroutine callback.
func (s *Server) AddTopic(topicARN, endpoint string, callback func(*Message)) {
	t := &topicDescription{
		TopicARN: topicARN,
		Callback: callback,
	}
	if endpoint[:1] != "/" {
		endpoint = "/" + endpoint
	}
	if s.topics == nil {
		s.topics = map[string]*topicDescription{
			endpoint: t,
		}
	} else {
		s.topics[endpoint] = t
	}
	if s.Logger != nil {
		s.Logger.Printf("Adding endpoint '%s' for topic '%s'\n", endpoint, topicARN)
	}
}

func simpleResponse(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(code)
	fmt.Fprintln(w, msg)
}

func (s *Server) extractJsonBody(r *http.Request) map[string]interface{} {
	var nbytes int
	n, err := fmt.Sscanf(r.Header.Get("Content-Length"), "%d", &nbytes)
	if n != 1 || err != nil {
		fmt.Printf("no content-length??\n%+v", r.Header)
		return nil
	}
	jsonBytes := make([]byte, nbytes, nbytes)
	n, err = io.ReadFull(r.Body, jsonBytes)
	if n != nbytes || err != nil {
		fmt.Printf("error reading body (%d,%d) %v", n, nbytes, err)
		return nil
	}

	data := make(map[string]interface{})
	if r.Header.Get("x-amz-sns-rawdelivery") == "true" {
		data["Subject"] = ""
		data["Message"] = string(jsonBytes)
		data["MessageId"] = r.Header.Get("x-amz-sns-message-id")
		data["Timestamp"] = time.Now().In(time.UTC).Format(amzTimeFormat)
		return data
	}

	err = json.Unmarshal(jsonBytes, &data)
	if err != nil {
		fmt.Printf("error parsing json body %v", err)
		return nil
	}

	return data
}

func (s *Server) confirmSub(td *topicDescription, r *http.Request) {
	data := s.extractJsonBody(r)
	if data == nil {
		return
	}

	subURL := data["SubscribeURL"].(string)
	_, err := http.Get(subURL)
	if err != nil {
		fmt.Printf("error confirming subscription: %v", err)
		return
	}
	r.Body.Close()

	if s.Logger != nil {
		s.Logger.Printf("Endpoint '%s' confirmed subscription for topic '%s'\n", r.URL.Path, td.TopicARN)
	}
	// ping callback to allow for init
	go td.Callback(nil)
}

func (s *Server) processMessage(td *topicDescription, r *http.Request) {
	data := s.extractJsonBody(r)
	if data == nil {
		return
	}

	timeStr := data["Timestamp"].(string)
	tm, _ := time.Parse(amzTimeFormat, timeStr)
	msg := &Message{
		Message:   data["Message"].(string),
		MessageId: data["MessageId"].(string),
		Timestamp: tm,
	}
	if data["Subject"] != nil {
		msg.Subject = data["Subject"].(string)
	}

	if s.Logger != nil {
		s.Logger.Printf("Endpoint '%s' got message for topic '%s':\n", r.URL.Path, td.TopicARN)
		s.Logger.Println("    MessageId: " + msg.MessageId)
	}
	go td.Callback(msg)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if td, found := s.topics[r.URL.Path]; found {
		// check that topic is configured correctly
		amzTopic := r.Header.Get("x-amz-sns-topic-arn")
		if td.TopicARN == amzTopic {
			// determine message type
			amzType := r.Header.Get("x-amz-sns-message-type")

			switch amzType {
			case "SubscriptionConfirmation":
				s.confirmSub(td, r)
				simpleResponse(w, http.StatusOK, "ok")
			case "Notification":
				s.processMessage(td, r)
				simpleResponse(w, http.StatusOK, "ok")
			default:
				simpleResponse(w, http.StatusNotImplemented, "not implemented")
			}
			return
		}

		// write out a 400
		simpleResponse(w, http.StatusBadRequest, "bad request")
		return
	}

	// write out a 404
	simpleResponse(w, http.StatusNotFound, "not found")
}

func (s *Server) ListenAndServe(address string) error {
	srv := &http.Server{
		Addr:           address,
		Handler:        s,
		ReadTimeout:    15 * time.Second,
		WriteTimeout:   15 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	if s.Logger != nil {
		s.Logger.Println("Listening on " + address)
	}
	return srv.ListenAndServe()
}
