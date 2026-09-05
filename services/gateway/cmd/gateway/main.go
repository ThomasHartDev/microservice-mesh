package main

import (
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/thomashartdev/microservice-mesh/services/gateway"
)

func main() {
	addr := ":8080"
	if v := os.Getenv("GATEWAY_ADDR"); v != "" {
		addr = v
	}
	if len(os.Args) > 1 && os.Args[1] == "-health" {
		host := addr
		if strings.HasPrefix(host, ":") {
			host = "127.0.0.1" + host
		}
		res, err := (&http.Client{Timeout: 2 * time.Second}).Get("http://" + host + "/healthz")
		if err != nil || res.StatusCode != http.StatusOK {
			os.Exit(1)
		}
		res.Body.Close()
		return
	}
	pub := gateway.LogPublisher{W: os.Stdout}
	srv := &http.Server{Addr: addr, Handler: gateway.New(pub).Handler()}
	log.Printf("gateway listening on %s", addr)
	log.Fatal(srv.ListenAndServe())
}
