#include <librdkafka/rdkafkacpp.h>
#include <Rcpp.h>
#include "utils.h"
#include <string>
#include <cstring>

////////////////////////////////////////////////////////////////////////////////////////
//' @title RdKafkaConsumer
//' @name RdKafkaConsumer
//' @description Creates an Rcpp::XPtr<RdKafka::Consumer>. For more details on options see \href{https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md}{librdkafka}.
//' @param properties string vector. Indicating option properties to parameterize the RdKafka::Consumer.
//' @param values string vector. Indicating option values to parameterize the RdKafka::Consumer. Must be of same length as properties.
//' @return Rcpp::XPtr<RdKafka::Consumer> pointer.
// [[Rcpp::export]]
SEXP RdKafkaConsumer(Rcpp::StringVector properties, Rcpp::StringVector values) {
    std::string errstr;
    auto conf = MakeKafkaConfig(properties, values);
    RdKafka::KafkaConsumer *consumer = RdKafka::KafkaConsumer::create(conf, errstr);
    if(!consumer) {
      Rcpp::stop("Consumer creation failed with error: " + errstr);
    }
    Rcpp::XPtr<RdKafka::KafkaConsumer> p(consumer, true);

    return p;
}

//' @title RdSubscribe
//' @name RdSubscribe
//' @description A method to register a consumer with a set amount of topics as consumers.
//' This is important so the broker can track offsets and register it in a consumer group.
//' @param consumerPtr pointer. A reference to a Rcpp::XPtr<RdKafka::KafkaConsumer>.
//' @param rTopics string vector. Listing the topics to subscribe to.
//' @return integer. Representation of the librdkafka error code of the response to subscribe. 0 is good.
// [[Rcpp::export]]
int RdSubscribe(SEXP consumerPtr, Rcpp::StringVector rTopics) {
    Rcpp::XPtr<RdKafka::KafkaConsumer> consumer(consumerPtr);
    std::vector<std::string> topics(rTopics.size());

    for (int i = 0; i < rTopics.size(); i++){
        topics[i] = rTopics(i);
    }
    RdKafka::ErrorCode resp = consumer->subscribe(topics);

    return static_cast<int>(resp);
}

//' @title RdConsume
//' @name RdConsume
//' @description Consume a fixed number of results from whatever topic(s) the provided consumer is subscribed to.
//' @param consumerPtr pointer. A reference to a Rcpp::XPtr<RdKafka::KafkaConsumer>.
//' @param numResults integer. How many results should be consumed before returning. Will return early if offset is at maximum.
//' @param timeoutMs integer. Number of milliseconds to wait for a new message.
//' @return list. With length numResults and elements topic, key and payload.
// [[Rcpp::export]]
Rcpp::List RdConsume(SEXP consumerPtr, int numResults, int timeoutMs) {
    Rcpp::XPtr<RdKafka::KafkaConsumer> consumer(consumerPtr);
    Rcpp::List messages(numResults);

    for(int i = 0; i < numResults; i++) {
        RdKafka::Message *msg = consumer->consume(timeoutMs);
        switch(msg->err()) {
            case RdKafka::ERR_NO_ERROR: {
                Rcpp::List message = Rcpp::List::create(Rcpp::Named("topic") = msg->topic_name(),
                                                        Rcpp::Named("key") = *msg->key(),
                                                        Rcpp::Named("payload") = static_cast<const char *>(msg->payload()));
                messages[i] = message;
                break;
            } case RdKafka::ERR__PARTITION_EOF: {
                Rcpp::Rcout << "No additional messages available" << std::endl;
                goto exit_loop;
            } case RdKafka::ERR__TIMED_OUT: {
                Rcpp::Rcout << "Timeout was reached with no new messages" << std::endl;
                goto exit_loop;
            } default: {
                /* Errors */
                Rcpp::Rcout << "Consume failed: " << msg->errstr().c_str() << std::endl;
                goto exit_loop;
            }
        }
    }
    exit_loop:;

    return messages;
}

//' @title RdAssign
//' @name RdAssign
//' @description In process
//' @param consumerPtr pointer. A reference to a Rcpp::XPtr<RdKafka::KafkaConsumer>.
//' @param topic string.
//' @param partition integer.
//' @return integer. Representation of the librdkafka error code of the response to subscribe. 0 is good.
// [[Rcpp::export]]
int RdAssign(SEXP consumerPtr, std::string topic, int partition, int offset) {
  Rcpp::XPtr<RdKafka::KafkaConsumer> consumer(consumerPtr);
  std::vector<RdKafka::TopicPartition*> partitions = { RdKafka::TopicPartition::create(topic, partition, offset) };

  RdKafka::ErrorCode resp = consumer->assign(partitions);

  return static_cast<int>(resp);
}

//' @title RdConsumePartition
//' @name RdConsumePartition
//' @description In process
//' @param consumerPtr pointer. A reference to a Rcpp::XPtr<RdKafka::KafkaConsumer>.
//' @param numResults integer. How many results should be consumed before returning. Will return early if offset is at maximum.
//' @param timeoutMs integer. Number of milliseconds to wait for a new message.
//' @return list. With length numReceived and elements topic, partition, offset, key and payload.
// [[Rcpp::export]]
Rcpp::List RdConsumePartition(SEXP consumerPtr, int numResults, int timeoutMs) {
  Rcpp::XPtr<RdKafka::KafkaConsumer> consumer(consumerPtr);
  Rcpp::List messages(numResults);

  for(int i = 0; i < numResults; i++) {
    RdKafka::Message *msg = consumer->consume(timeoutMs);
    switch(msg->err()) {
    case RdKafka::ERR_NO_ERROR: {
      Rcpp::List message = Rcpp::List::create(Rcpp::Named("topic") = msg->topic_name(),
                                              Rcpp::Named("partition") = msg->partition(),
                                              Rcpp::Named("offset") = msg->offset(),
                                              Rcpp::Named("key") = *msg->key(),
                                              Rcpp::Named("payload") = static_cast<const char *>(msg->payload()));
      messages[i] = message;
      break;
    } case RdKafka::ERR__PARTITION_EOF: {
      Rcpp::Rcout << "No additional messages available" << std::endl;
      goto exit_loop;
    } case RdKafka::ERR__TIMED_OUT: {
      Rcpp::Rcout << "Timeout was reached with no new messages" << std::endl;
      goto exit_loop;
    } default: {
      /* Errors */
      Rcpp::Rcout << "Consume failed: " << msg->errstr().c_str() << std::endl;
      goto exit_loop;
    }
    }
  }
  exit_loop:;

  return messages;
}
