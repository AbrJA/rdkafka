#' @importFrom R6 R6Class
#' @title Kakfa Producer
#' @name KafkaProducer
#' @description A producer is an application that is responsible for publishing data to topics.
#' @references \href{https://kafka.apache.org/documentation/#intro_producers}{Apache Kafka docs - Producers}
#' @importFrom R6 R6Class
#' @export
KafkaProducer <- R6::R6Class(
    classname = "KafkaProducer",
    public = list(
        #-----------------------------------------------------------------
        #' @param brokers string vector. Initial list of brokers with the structure broker host or host:port.
        #' @param extra_options list. Indicating option properties and option values to parameterize the RdKafka::Producer.
        #'
        #' @return invisible logical. `TRUE` if all went good.
        #' @export
        initialize = function(brokers = "localhost:9092", extra_options = list()) {
            stopifnot(is.character(brokers), is.character(group_id), is.list(extra_options))
            properties <- c("metadata.broker.list", names(extra_options))
            values <- c(paste0(brokers, collapse = ","), unlist(extra_options, use.names = FALSE))
            private$producer_ptr <- RdKafkaProducer(properties, values)
            private$brokers <- brokers
            invisible(TRUE)
        },
        #-----------------------------------------------------------------
        #' @param keys string vector. With all the keys for the messages.
        #' @param payloads string vector. With all the payloads for the messages. Must be of same length as keys.
        #' @param topics string vector. Indicating the topics to produce to. Must be of same length as keys or length equal 1.
        #' @param partitions integer vector. Indicating the partitions to produce to. Must be of same length as keys or length equal 1.
        #'
        #' @return invisible integer. Number of messages succesfully sent.
        #' @export
        produce = function(keys, payloads, topics, partitions = rep.int(0L, length(keys))) {
            stopifnot(is.character(keys), is.character(payloads), is.character(topics), is.numeric(partitions))
            if (length(topics) == 1L) topics <- rep.int(topics, length(keys))
            if (length(partitions) == 1L) partitions <- rep.int(partitions, length(keys))
            stopifnot(length(payloads) == length(keys), length(topics) == length(keys), length(partitions) == length(keys))
            invisible(RdProduce(private$producer_ptr, keys, payloads, topic, partitions))
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
        brokers = NULL,
        producer_ptr = NULL
    )
)
