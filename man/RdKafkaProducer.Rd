% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/RcppExports.R
\name{RdKafkaProducer}
\alias{RdKafkaProducer}
\title{RdKafkaProducer}
\usage{
RdKafkaProducer(properties, values)
}
\arguments{
\item{properties}{string vector. Indicating option properties to parameterize the RdKafka Producer.}

\item{values}{string vector. Indicating option values to parameterize the RdKafka Producer. Must be of same length as properties.}
}
\value{
RdKafka Producer pointer.
}
\description{
Creates a pointer to a RdKafka Producer. For more details on options see \href{https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md}{librdkafka}.
}
