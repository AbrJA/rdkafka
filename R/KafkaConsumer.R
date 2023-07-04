#' @importFrom R6 R6Class
#' @title Kakfa Consumer
#' @name KafkaConsumer
#' @description A consumer is an application which subscribes to one or more topics and processes
#' new messages as they arrive on that topic.
#' Consumers may also be instructed to process older messages.
#' Consumers belong to "consumer groups". Consumer groups with more than one consumer instance can be
#' used to parallelize message processing and / or distribute it over multiple physical machines.
#' @references \href{https://kafka.apache.org/documentation/#intro_consumers}{Apache Kafka docs - Consumers}
#' @importFrom R6 R6Class
#' @export
#' @examples
#' \dontrun{
#' library(rdkafka)
#'
#' # KafkaProducer
#' producer <- KafkaProducer$new(host = "localhost", port = 9092)
#' producer$produce(topic = "MyTest",
#'                  key = "Message 1",
#'                  value = "My First Message")
#' # Number of messages successfuly sent is returned
#' # [1] 1
#'
#'
#' # KafkaConsumer
#' consumer <- KafkaConsumer$new(host = "localhost", port = 9092, group_id = "test",
#' extra_options = list(`auto.offset.reset` = "earliest"))
#' consumer$subscribe(topics = "MyTest")
#' result <- consumer$consume(topic = "MyTest")
#'
#' result
#' # Consumed messages are returned in a list(list(key,val)) format
#' # [[1]]
#' # [[1]]$key
#' # [1] "myKey"
#' #
#' # [[1]]$payload
#' # [1] "My First Message"
#'}
KafkaConsumer <- R6::R6Class(
    classname = "KafkaConsumer",
    public = list(
        #-----------------------------------------------------------------
        #' @param host string
        #' @param port string
        #' @param group_id string
        #' @param extra_options list
        #'
        #' @return invisible. Logical. `TRUE` if all went good.
        #' @export
        initialize = function(host, port, group_id, extra_options = list()) {
            stopifnot(length(host) == length(port) || length(port) == 1)
            private$host <- host
            private$port <- port
            properties <- c("metadata.broker.list", "group.id", names(extra_options))
            values <- c(self$getHostPort(), group_id, unlist(extra_options, use.names = FALSE))
            private$consumer_ptr <- GetRdConsumer(properties, values)
        },
        #-----------------------------------------------------------------
        #' @param topics string vector
        #'
        #' @return @return invisible. Logical. `TRUE` if all went good.
        #' @export
        subscribe = function(topics) {
            stopifnot(is.character(topics))
            result <- RdSubscribe(private$consumer_ptr, topics)
            if (result == 0) {
                private$topics <- c(private$topics, topic)
            }
        },
        #-----------------------------------------------------------------
        #' @param topic string
        #' @param num_results integer
        #' @param timeout_ms integer
        #'
        #' @return invisible. Logical. `TRUE` if all went good.
        #' @export
        consume = function(num_results = 100, timeout_ms = 1000) {
            stopifnot(is.numeric(num_results), is.numeric(timeout_ms))
            #if (is.null(private$topics)) stop("Consumer is not suscribed to any topic")
            Filter(function(msg) !is.null(msg), KafkaConsume(private$consumer_ptr, num_results, timeout_ms))
        },
        #-----------------------------------------------------------------
        #' @return invisible. Logical. `TRUE` if all went good.
        #' @export
        getTopics = function() {
            private$topics
        },
        #-----------------------------------------------------------------
        #' @return invisible. Logical. `TRUE` if all went good.
        #' @export
        getHostPort = function() {
            stopifnot(!is.null(private$host), !is.null(private$port))
            paste0(private$host, ":", private$port)
        }
    ),
    private = list(
        host = NULL,
        port = NULL,
        topics = NULL,
        consumer_ptr = NULL
    )
)
