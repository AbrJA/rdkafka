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
        initialize = function(brokers, extra_options = list()) {
            stopifnot(is.character(brokers))
            properties <- c("metadata.broker.list", names(extra_options))
            values <- c(paste0(brokers, collapse = ","), unlist(extra_options, use.names = FALSE))
            private$producer_ptr <- RdKafkaProducer(properties, values)
            private$brokers <- brokers
            invisible(TRUE)
        },
        # Produce single message to topic
        #-----------------------------------------------------------------
        #' @param topic string. Indicating the topic to produce to.
        #' @param keys string vector. With all the keys for the messages.
        #' @param payloads string vector. With all the payloads for the messages. Must be of same length as keys.
        #' @param partition integer. Indicating the partition to produce to.
        #'
        #' @return invisible integer. Number of messages succesfully sent.
        #' @export
        produce = function(topic, keys, payloads, partition = 0) {
            stopifnot(is.character(topic), length(topic) == 1, is.character(keys), is.character(payloads), is.numeric(partition), length(partition) == 1)
            invisible(RdProduce(private$producer_ptr, topic, partition, keys, payloads))
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
