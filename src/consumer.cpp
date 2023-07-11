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
SEXP RdKafkaConsumer(Rcpp::StringVector properties,
                     Rcpp::StringVector values) {
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
//' @param topics string vector. Listing the topics to subscribe to.
//' @return integer. Representation of the librdkafka error code of the response to subscribe. 0 is good.
// [[Rcpp::export]]
int RdSubscribe(SEXP consumerPtr,
                Rcpp::StringVector topics) {
  Rcpp::XPtr<RdKafka::KafkaConsumer> consumer(consumerPtr);
  std::vector<std::string> topicsStr(topics.size());

  for (int i = 0; i < topics.size(); i++) {
    topicsStr[i] = topics(i);
  }

  RdKafka::ErrorCode resp = consumer->subscribe(topicsStr);

  return static_cast<int>(resp);
}

//' @title RdAssign
//' @name RdAssign
//' @description In process
//' @param consumerPtr pointer. A reference to a Rcpp::XPtr<RdKafka::KafkaConsumer>.
//' @param topics string vector. Listing the topics to subscribe to.
//' @param partitions integer vector.
//' @param offsets integer vector.
//' @return integer. Representation of the librdkafka error code of the response to subscribe. 0 is good.
// [[Rcpp::export]]
int RdAssign(SEXP consumerPtr,
             Rcpp::StringVector topics,
             Rcpp::IntegerVector partitions,
             Rcpp::IntegerVector offsets) {
  Rcpp::XPtr<RdKafka::KafkaConsumer> consumer(consumerPtr);
  std::vector<RdKafka::TopicPartition*> topicPartitions;

  for (int i = 0; i < topics.size(); i++) {
    std::string topicStr = Rcpp::as<std::string>(topics[i]);
    int partitionInt = partitions[i];
    int offsetInt = offsets[i];
    topicPartitions.push_back(RdKafka::TopicPartition::create(topicStr, partitionInt, offsetInt));
  }

  RdKafka::ErrorCode resp = consumer->assign(topicPartitions);

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
Rcpp::List RdConsume(SEXP consumerPtr,
                     int numResults,
                     int timeoutMs) {
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
          Rcpp::Rcout << "Consume failed: " << msg->errstr().c_str() << std::endl;
          goto exit_loop;
      }
    }
  }
  exit_loop:;

  return messages;
}
