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
        #' @param host string
        #' @param port string
        #' @param extra_options list
        #'
        #' @return @return invisible. Logical. `TRUE` if all went good.
        #' @export
        initialize = function(host, port, extra_options = list()) {
            stopifnot(length(host) == length(port) || length(port) == 1)
            private$host <- host
            private$port <- port
            properties <- c("metadata.broker.list", names(extra_options))
            values <- c(self$getHostPort(), unlist(extra_options, use.names = FALSE))
            private$producer_ptr <- GetRdProducer(properties, values)
        },
        # Produce single message to topic
        #-----------------------------------------------------------------
        #' @param topic string
        #' @param keys string vector
        #' @param values string vector
        #' @param partition integer
        #'
        #' @return invisible. Logical. `TRUE` if all went good.
        #' @export
        produce = function(topic, keys, values, partition = 0) {
            stopifnot(is.character(topic), length(topic) == 1, is.character(keys), is.character(values), is.numeric(partition), length(partition) == 1)
            KafkaProduce(private$producer_ptr, topic, partition, keys, values)
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
        producer_ptr = NULL
    )
)
