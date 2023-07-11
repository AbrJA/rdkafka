#include <librdkafka/rdkafkacpp.h>
#include <Rcpp.h>
#include "utils.h"
#include <string>
#include <cstring>

//' @title RdKafkaProducer
//' @name RdKafkaProducer
//' @description Creates an Rcpp::XPtr<RdKafka::Producer>. For more details on options see \href{https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md}{librdkafka}.
//' @param properties string vector. Indicating option properties to parameterize the RdKafka::Producer.
//' @param values string vector. Indicating option values to parameterize the RdKafka::Producer. Must be of same length as properties.
//' @return Rcpp::XPtr<RdKafka::Producer> pointer.
// [[Rcpp::export]]
SEXP RdKafkaProducer(Rcpp::StringVector properties,
                     Rcpp::StringVector values) {
  std::string errstr;
  auto conf = MakeKafkaConfig(properties, values);
  RdKafka::Producer *producer = RdKafka::Producer::create(conf, errstr);

  if(!producer) {
    Rcpp::stop("Producer creation failed with error: " + errstr);
  }

  Rcpp::XPtr<RdKafka::Producer> p(producer, true);

  return p;
}

//' @title RdProduce
//' @name RdProduce
//' @description Produces key/values to a particular topic on a particular partition.
//' @param producerPtr pointer. A reference to a Rcpp::XPtr<RdKafka::Producer>
//' @param keys string vector. With all the keys for the messages.
//' @param payloads string vector. With all the payloads for the messages. Must be of same length as keys.
//' @param topics string vector. Indicating the topics to produce to.
//' @param partition integer vector. Indicating the partition to produce to.
//' @return returns integer. Number of messages succesfully sent.
// [[Rcpp::export]]
int RdProduce(SEXP producerPtr,
              Rcpp::StringVector keys,
              Rcpp::StringVector payloads,
              Rcpp::StringVector topics,
              Rcpp::IntegerVector partitions) {
  Rcpp::XPtr<RdKafka::Producer> producer(producerPtr);
  int numSent = 0;

  for (int i = 0; i < keys.size(); i++) {
    std::string topicStr = Rcpp::as<std::string>(topics[i]);
    std::string payloadStr = Rcpp::as<std::string>(payloads[i]);
    std::string keyStr = Rcpp::as<std::string>(keys[i]);
    int partition = partitions[i];

    RdKafka::ErrorCode resp = producer->produce(
      topicStr, partition, RdKafka::Producer::RK_MSG_COPY,
      const_cast<char *>(payloadStr.c_str()), payloadStr.size(),
      const_cast<char *>(keyStr.c_str()), keyStr.size(),
      0, NULL
    );

    if (resp == RdKafka::ERR_NO_ERROR) numSent++;
  }

  producer->flush(0);

  return numSent;
}
