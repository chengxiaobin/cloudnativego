package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

/* func hello(w http.ResponseWriter, r *http.Request){
	w.Write([]byte("Hello, Cloud Native"))
} */

var transact TransactionLogger

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Println(r.Method, r.RequestURI)
		next.ServeHTTP(w, r)
	})
}

func notAllowedHandler(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Not Allowed", http.StatusMethodNotAllowed)
}

func helloMux(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("Hello, Mux\n"))
}

func putHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]
	value, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	err = Put(key, string(value))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	transact.WritePut(key, string(value))
	w.WriteHeader(http.StatusCreated)
	log.Printf("PUT key=%s value=%s\n", key, string(value))
}

func delHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]
	err := Delete(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	transact.WriteDelete(key)

	log.Printf("DELETE key=%s\n", key)
}

func getHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := Get(key)
	if errors.Is(err, ErrorNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write([]byte(value))
	log.Printf("GET key=%s\n", key)
}

/* func initializeTransactionLog() error {
	var err error

	transact, err = NewTransactionLogger("transactions.log")
	if err != nil {
		return fmt.Errorf("failed to create transaction logger: %w", err)
	}

	events, errors := transact.ReadEvents()
	count, ok, e := 0, true, Event{}

	for ok && err == nil {
		select {
		case err, ok = <-errors:

		case e, ok = <-events:
			switch e.EventType {
			case EventDelete: // Got a DELETE event!
				err = Delete(e.Key)
				count++
			case EventPut: // Got a PUT event!
				err = Put(e.Key, e.Value)
				count++
			}
		}
	}

	log.Printf("%d events replayed\n", count)

	transact.Run()

	return err
} */

func initializeTransactionLog() error {
	var err error
   
	transact, err = NewMysqlTransactionLogger(MysqlDbParams{
	 host:     "192.168.89.55",
	 dbName:   "test",
	 user:     "root",
	 password: "gh2170onCZ",
	})
	if err != nil {
	 return fmt.Errorf("failed to create transaction logger: %w", err)
	}
   
	events, errors := transact.ReadEvents()
	count, ok, e := 0, true, Event{}
   
	for ok && err == nil {
	 select {
	 case err, ok = <-errors:
   
	 case e, ok = <-events:
	  switch e.EventType {
	  case EventDelete: // Got a DELETE event!
	   err = Delete(e.Key)
	   count++
	  case EventPut: // Got a PUT event!
	   err = Put(e.Key, e.Value)
	   count++
	  }
	 }
	}
   
	log.Printf("%d events replayed\n", count)
   
	transact.Run()
   
	go func() {
	 for err := range transact.Err() {
	  log.Print(err)
	 }
	}()
   
	return err
   }

func main() {
	//http.HandleFunc("/", hello)
	err := initializeTransactionLog()
	if err != nil {
		panic(err)
	}
	r := mux.NewRouter()

	r.Use(loggingMiddleware)

	r.HandleFunc("/v1/{key}", getHandler).Methods("GET")
	r.HandleFunc("/v1/{key}", putHandler).Methods("PUT")
	r.HandleFunc("/v1/{key}", delHandler).Methods("DELETE")

	r.HandleFunc("/v1", notAllowedHandler)
	r.HandleFunc("/v1/{key}", notAllowedHandler)

	log.Fatal(http.ListenAndServe(":8080", r))
}
