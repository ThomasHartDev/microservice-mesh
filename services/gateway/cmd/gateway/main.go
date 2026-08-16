package main

import (
	"log"
	"net/http"
	"os"

	"github.com/thomashartdev/microservice-mesh/services/gateway"
)

func main() {
	addr := ":8080"
	if v := os.Getenv("GATEWAY_ADDR"); v != "" {
		addr = v
	}
	pub := gateway.LogPublisher{W: os.Stdout}
	srv := &http.Server{Addr: addr, Handler: gateway.New(pub).Handler()}
	log.Printf("gateway listening on %s", addr)
	log.Fatal(srv.ListenAndServe())
}
