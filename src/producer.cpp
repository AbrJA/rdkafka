#include <librdkafka/rdkafkacpp.h>
#include <Rcpp.h>
#include "utils.h"
#include <iostream>
#include <string>
#include <cstdlib>
#include <cstdio>
#include <csignal>
#include <cstring>

//' @title RdKafkaProducer
//' @name RdKafkaProducer
//' @description Creates an Rcpp::XPtr<RdKafka::Producer>. For more details on options see \href{https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md}{librdkafka}.
//' @param properties string vector. Indicating option properties to parameterize the RdKafka::Producer.
//' @param values string vector. Indicating option values to parameterize the RdKafka::Producer. Must be of same length as properties.
//' @return Rcpp::XPtr<RdKafka::Producer> pointer.
// [[Rcpp::export]]
SEXP RdKafkaProducer(Rcpp::StringVector properties, Rcpp::StringVector values) {
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
//' @param topic string. Indicating the topic to produce to.
//' @param partition integer. Indicating the partition to produce to.
//' @param keys string vector. With all the keys for the messages.
//' @param payloads string vector. With all the payloads for the messages. Must be of same length as keys.
//' @return returns integer. Number of messages succesfully sent.
// [[Rcpp::export]]
int RdProduce(SEXP producerPtr,
            SEXP topic,
            Rcpp::IntegerVector partition,
            Rcpp::StringVector keys,
            Rcpp::StringVector payloads) {
    Rcpp::XPtr<RdKafka::Producer> producer(producerPtr);
    std::string s_topic = Rcpp::as<std::string>(topic);

    if (keys.size() != payloads.size()) {
      Rcpp::Rcout << "keys and payloads must be same size" << std::endl;
      return -1;
    }
    int numMsgs = keys.size();
    int numSent = 0;

    for (int i = 0; i < numMsgs; i++) {
        std::string s_value = Rcpp::as<std::string>(payloads[i]);
        std::string s_key = Rcpp::as<std::string>(keys[i]);

        RdKafka::ErrorCode resp = producer->produce(
            s_topic, partition[0], RdKafka::Producer::RK_MSG_COPY,
            const_cast<char *>(s_value.c_str()), s_value.size(),
            const_cast<char *>(s_key.c_str()), s_key.size(),
            0, NULL
        );

        if (resp == RdKafka::ERR_NO_ERROR) numSent++;
    }

    producer->flush(0);

    return numSent;
}
