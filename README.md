
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

**Note:** Only tested on linux at the moment.

## Example

### Previously

Start the Kafka broker with the `docker compose` command:

``` r
(sudo) docker compose up -d
```

**Note:** Make sure you are in the directory containing the
`docker-compose.yml` file.

Create the example topics `Topic1` and `Topic2` with the following
command:

``` r
(sudo) docker compose exec broker \
  kafka-topics --create \
    --topic Topic1 \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1
```

Use a `KafkaProducer` object to send messages and a `Kafka Consumer` to
receive them:

``` r
library(rdkafka)

producer <- KafkaProducer$new(host = "localhost", port = 9092)
consumer <- KafkaConsumer$new(host = "localhost", port = 9092, group_id = "readme", extra_options = list("auto.offset.reset" = "earliest"))
```

``` r
counter <- seq_len(5L)
producer$produce(topic = "Topic1", keys = sprintf("Key %s", counter), values = sprintf("Message %s", counter)) |> print()
#> [1] 5
producer$produce(topic = "Topic2", keys = sprintf("Id %s", counter), values = sprintf("Body %s", counter)) |> print()
#> [1] 5
```

``` r
consumer$subscribe(topics = c("Topic1", "Topic2"))
consumer$get_topics()
#> [1] "Topic1" "Topic2"
```

``` r
results <- list()
while (identical(results, list())) {
  results <- consumer$consume(num_results = 10, timeout_ms = 1000)
}
#> Timeout was reached with no new messages
#> Timeout was reached with no new messages
data.table::rbindlist(results)
#>      topic   key     value
#>  1: Topic1 Key 1 Message 1
#>  2: Topic1 Key 2 Message 2
#>  3: Topic1 Key 3 Message 3
#>  4: Topic1 Key 4 Message 4
#>  5: Topic1 Key 5 Message 5
#>  6: Topic2  Id 1    Body 1
#>  7: Topic2  Id 2    Body 2
#>  8: Topic2  Id 3    Body 3
#>  9: Topic2  Id 4    Body 4
#> 10: Topic2  Id 5    Body 5
```
