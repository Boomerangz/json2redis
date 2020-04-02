package main

type Config struct {
	Port          int
	Redis         string
	RedisPassword string
	RedisDB       int
	Buffersize    int
}
