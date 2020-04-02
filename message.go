package main

type Message struct {
	UUID string `json:"uuid"`
	Data []byte `json:"-"`
}
