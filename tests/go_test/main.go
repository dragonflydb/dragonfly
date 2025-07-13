package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"time"
)

type debugCache struct {
	tls.ClientSessionCache
}

func (d *debugCache) Put(key string, cs *tls.ClientSessionState) {
	fmt.Printf("Session ticket stored for key: %s\n", key)
	d.ClientSessionCache.Put(key, cs)
}

func main() {
	// Load CA certificate
	caCert, err := ioutil.ReadFile("/path/to/ca-cert.pem")
	if err != nil {
		log.Fatalf("Failed to read CA cert: %v", err)
	}
	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		log.Fatalf("Failed to append CA cert")
	}

	// Load client certificate and key
	clientCert, err := tls.LoadX509KeyPair("path/to/cl-cert.pem", "path/to/cl-key.pem")
	if err != nil {
		log.Fatalf("Failed to load client certificate/key: %v", err)
	}

	// TLS config with session cache, CA, and client cert
	cache := &debugCache{tls.NewLRUClientSessionCache(32)}
	config := &tls.Config{
		MinVersion:         tls.VersionTLS13,
		MaxVersion:         tls.VersionTLS13,
		RootCAs:            caCertPool,
		Certificates:       []tls.Certificate{clientCert},
		ClientSessionCache: cache,
		InsecureSkipVerify: true,
	}

	address := "localhost:6379" // Change to your server address

	// First connection
	conn, err := tls.Dial("tcp", address, config)
	if err != nil {
		log.Fatalf("First dial failed: %v", err)
	}
	fmt.Println("First handshake complete")

	err = conn.Handshake() // Ensures handshake is complete now
	if err != nil {
		// handle handshake error
		log.Fatalf("First dial failed: %v", err)
	}
	// Try to read from the connection to progress the state machine
	conn.Write([]byte("ping"))

	buf := make([]byte, 1)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second)) // Avoid blocking forever
	_, err = conn.Read(buf)
	if err != nil {
		// handle handshake error
		//log.Fatalf("read failed")

	}

	// Wait before reconnecting
	time.Sleep(1 * time.Second)
	fmt.Printf("Session cache stats: %+v\n", cache)

	// Second connection to test session resumption
	conn2, err := tls.Dial("tcp", address, config)
	if err != nil {
		log.Fatalf("Second dial failed: %v", err)
	}
	defer conn2.Close()

	// **********************************************************************
	// Run this in a loop and see the difference via perf when the session is
	// reused and the handshake is skipped vs when handshake is done on every
	// reconnect.

	// Check if handshake was resumed (skipped)
	state := conn2.ConnectionState()
	if state.DidResume {
		fmt.Println("Handshake was resumed (skipped full handshake)")
	} else {
		fmt.Println("Full handshake was performed")
	}
}
