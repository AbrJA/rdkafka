library(devtools)
install()

library(rdkafka)

# KafkaProducer
producer <- KafkaProducer$new(host = "localhost", port = 9092)
for (counter in seq_len(5)) {
  producer$produce(topic = "Topic1", keys = sprintf("Key %s - Topic 1", counter), values = sprintf("Message %s - Topic 1", counter)) |> print()
  producer$produce(topic = "Topic2", keys = sprintf("Key %s - Topic 2", counter), values = sprintf("Message %s - Topic 2", counter)) |> print()
}
# Number of messages successfuly sent is returned
# [1] 1

# KafkaConsumer
consumer <- KafkaConsumer$new(host = "localhost", port = 9092, group_id = "r", extra_options = list("auto.offset.reset" = "earliest"))
consumer$subscribe(topics = c("Topic1", "Topic2"))
consumer$consume(num_results = 10, timeout_ms = 1000)
