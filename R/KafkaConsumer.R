#' @importFrom R6 R6Class
#' @title Kakfa Consumer
#' @name KafkaConsumer
#' @description A consumer is an application which subscribes to one or more topics and processes new messages as they arrive on that topic.
#' @references \href{https://kafka.apache.org/documentation/#intro_consumers}{Apache Kafka docs - Consumers}
#' @importFrom R6 R6Class
#' @export
#' @examples
#' \dontrun{
#' library(rdkafka)
#'
#' # KafkaProducer
#' producer <- KafkaProducer$new()
#' producer$produce(topics = "MyTest", keys = "Message 1", payloads = "My First Message")
#'
#' # KafkaConsumer
#' consumer <- KafkaConsumer$new(extra_options = list(`auto.offset.reset` = "earliest"))
#' consumer$subscribe(topics = "MyTest")
#' result <- consumer$consume()
#' result
#'}
KafkaConsumer <- R6::R6Class(
    classname = "KafkaConsumer",
    public = list(
        #-----------------------------------------------------------------
        #' @param brokers string vector. Initial list of brokers with the structure broker host or host:port.
        #' @param group_id string. Client group id. All clients sharing the same group.id belong to the same group.
        #' @param extra_options list. Indicating option properties and option values to parameterize the RdKafka::Consumer.
        #'
        #' @return invisible logical. `TRUE` if all went good.
        #' @export
        initialize = function(brokers = "localhost:9092", group_id = "rdkafka", extra_options = list()) {
            stopifnot(is.character(brokers), is.character(group_id), is.list(extra_options), length(group_id) == 1L)
            properties <- c("metadata.broker.list", "group.id", names(extra_options))
            values <- c(paste0(brokers, collapse = ","), group_id, unlist(extra_options, use.names = FALSE))
            private$consumer_ptr <- RdKafkaConsumer(properties, values)
            private$brokers <- brokers
            invisible(TRUE)
        },
        #-----------------------------------------------------------------
        #' @param topics string vector. Listing the topics to subscribe to.
        #'
        #' @return invisible integer. Representation of the `librdkafka` error code of the response to subscribe. 0 is good.
        #' @export
        subscribe = function(topics = "Topic") {
            stopifnot(is.character(topics))
            result <- RdSubscribe(private$consumer_ptr, topics)
            if (result == 0L) {
                private$offsets <- NULL
                private$partitions <- NULL
                private$topics <- topics
            }
            invisible(result)
        },
        #-----------------------------------------------------------------
        #' @param topics string vector. Listing the topics to subscribe to.
        #' @param partitions integer vector. Indicating the partitions to subscribe to. Must be of same length as topics or length equal 1.
        #' @param offsets integer vector. With the offsets where to start. Must be of same length as topics or length equal 1.
        #'
        #' @return invisible integer. Representation of the `librdkafka` error code of the response to subscribe. 0 is good.
        #' @export
        assign = function(topics = "Topic", partitions = 0L, offsets = 0L) {
            stopifnot(is.character(topics), is.numeric(partitions), is.numeric(offsets))
            if (length(partitions) == 1L) partitions <- rep.int(partitions, length(topics))
            if (length(offsets) == 1L) offsets <- rep.int(offsets, length(topics))
            stopifnot(length(partitions) == length(topics), length(offsets) == length(topics))
            result <- RdAssign(private$consumer_ptr, topics, partitions, offsets)
            if (result == 0L) {
                private$offsets <- offsets
                private$partitions <- partitions
                private$topics <- topics
            }
            invisible(result)
        },
        #-----------------------------------------------------------------
        #' @param num_results integer. How many results should be consumed before returning. Will return early if offset is at maximum.
        #' @param timeout_ms integer. Number of milliseconds to wait for a new message.
        #'
        #' @return list. Messages consumed with elements topic, key and payload.
        #' @export
        consume = function(num_results = 100L, timeout_ms = 1000L) {
            stopifnot(is.numeric(num_results), is.numeric(timeout_ms))
            if (is.null(private$topics)) stop("Consumer is not suscribed to any topic")
            Filter(function(msg) !is.null(msg), RdConsume(private$consumer_ptr, num_results, timeout_ms))
        },
        #-----------------------------------------------------------------
        #' @return string vector. Listing the offsets where to start.
        #' @export
        get_offsets = function() {
            private$offsets
        },
        #-----------------------------------------------------------------
        #' @return string vector. Listing the partitions subscribed to.
        #' @export
        get_partitions = function() {
            private$partitions
        },
        #-----------------------------------------------------------------
        #' @return string vector. Listing the topics subscribed to.
        #' @export
        get_topics = function() {
            private$topics
        },
        #-----------------------------------------------------------------
        #' @return string vector. List of brokers with the structure broker host or host:port.
        #' @export
        get_brokers = function() {
            brokers <- strsplit(private$brokers, ",", fixed = TRUE)
            brokers <- unlist(brokers, use.names = FALSE)
            unique(trimws(brokers))
        }
    ),
    private = list(
        offsets = NULL,
        partitions = NULL,
        topics = NULL,
        brokers = NULL,
        consumer_ptr = NULL
    )
)
