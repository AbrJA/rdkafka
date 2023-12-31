% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/RcppExports.R
\name{RdSubscribe}
\alias{RdSubscribe}
\title{RdSubscribe}
\usage{
RdSubscribe(consumerPtr, topics)
}
\arguments{
\item{consumerPtr}{pointer. A reference to a RdKafka KafkaConsumer.}

\item{topics}{string vector. Listing the topics to subscribe to.}
}
\value{
integer. Representation of the librdkafka error code of the response to subscribe. 0 is good.
}
\description{
A method to register a consumer with a set amount of topics as consumers.
This is important so the broker can track offsets and register it in a consumer group.
}
