
<!-- README.md is generated from README.Rmd. Please edit that file -->

# rdkafka

<!-- badges: start -->
<!-- badges: end -->

The goal of `rdkafka` is to work as a R wrapper for
[librdkafka](https://github.com/confluentinc/librdkafka)

## Installation

You can install the development version of `rdkafka` from
[GitHub](https://github.com/AbrJA/rdkafka) like so:

``` r
install.packages("devtools")
devtools::install_github("AbrJA/rdkafka")
```

## Example

This is a basic example which shows you how to solve a common problem:

``` r
library(rdkafka)

# KafkaProducer
producer <- KafkaProducer$new(host = "localhost", port = 9092)
for (counter in seq_len(5)) {
  producer$produce(topic = "Topic1", keys = sprintf("Key1 %s", counter), values = sprintf("Message1 %s", counter)) |> print()
  producer$produce(topic = "Topic2", keys = sprintf("Key2 %s", counter), values = sprintf("Message2 %s", counter)) |> print()
}

# KafkaConsumer
consumer <- KafkaConsumer$new(host = "localhost", port = 9092, group_id = "r", extra_options = list("auto.offset.reset" = "earliest"))
consumer$subscribe(topics = c("Topic1", "Topic2"))
results <- consumer$consume(num_results = 10, timeout_ms = 100)
data.table::rbindlist(results)
```