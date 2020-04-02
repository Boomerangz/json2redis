package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/spf13/viper"
	"github.com/valyala/fasthttp"
)

var redisClient *redis.Client
var messagesPool = sync.Pool{
	New: func() interface{} { return &Message{} },
}

func main() {
	viper.SetConfigName("conf")
	viper.SetConfigType("yaml") // REQUIRED if the config file does not have the extension in the name
	viper.AddConfigPath(".")    // path to look for the config file in
	viper.AutomaticEnv()
	viper.SetEnvPrefix("JSON2REDIS")
	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
	var config Config
	err = viper.Unmarshal(&config)
	if err != nil {
		panic(fmt.Sprintf("unable to decode into struct, %v", err))
	}

	redisClient = redis.NewClient(&redis.Options{
		Addr:     config.Redis,
		Password: config.RedisPassword,
		DB:       config.RedisDB,
	})

	_, err = redisClient.Ping().Result()
	if err != nil {
		log.Fatalf("Redis is not accessible: %s", err.Error())
	}

	bodies := make(chan []byte, config.Buffersize)
	go handleBodies(bodies, config)

	handlePost := func(ctx *fasthttp.RequestCtx) {
		ctx.SetStatusCode(200)
		if ctx.IsPost() {
			bodies <- ctx.PostBody()
		}
	}
	s := &fasthttp.Server{
		Handler: handlePost,
	}
	if err := s.ListenAndServe(fmt.Sprintf("127.0.0.1:%d", config.Port)); err != nil {
		log.Fatalf("error in ListenAndServe: %s", err)
	}
}

func handleBodies(ch chan []byte, config Config) {
	buffer := map[string]*Message{}
	previousTime := time.Now()
	ticker := time.NewTicker(time.Second)
	tryFlush := func() {
		if len(buffer) >= config.Buffersize || (time.Since(previousTime) > time.Second && len(buffer) > 0) {
			go flushBuffer(buffer)
			buffer = map[string]*Message{}
			previousTime = time.Now()
		}
	}
	for {
		select {
		case body := <-ch:
			{
				message := messagesPool.Get().(*Message)
				err := json.Unmarshal(body, message)
				if err != nil {
					log.Printf("error decoding body: %s\n", err.Error())
				}
				message.Data = body
				buffer[message.UUID] = message
				tryFlush()
			}
		case <-ticker.C:
			{
				tryFlush()
			}
		}
	}
}

func flushBuffer(buffer map[string]*Message) {
	pairs := make([]interface{}, 0, len(buffer)*2)
	for k, v := range buffer {
		pairs = append(pairs, k)
		pairs = append(pairs, v.Data)
		v.Data = v.Data[:0]
		messagesPool.Put(v)
	}
	status := redisClient.MSet(pairs...)
	if err := status.Err(); err != nil {
		log.Printf("error setting to redis: %s\n", err.Error())
	}
}
