package com.hogly.kafka;

import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class ActorConsumer extends UntypedActor {

  LoggingAdapter LOG = Logging.getLogger(getContext().system(), this);
  public static String START_READ = "start read messages from kafka";
  private KafkaConsumer<String, String> kafkaConsumer;

  public ActorConsumer() {
    this.kafkaConsumer = buildkafkaConsumer();
  }

  private KafkaConsumer<String,String> buildkafkaConsumer() {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "rep-view-2");
    props.put("enable.auto.commit", "true");
    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Arrays.asList("REP"));
    return consumer;
  }

  public static Props props() {
    return Props.create(ActorConsumer.class);
  }

  @Override
  public void onReceive(Object message) throws Exception {
    if (START_READ.equals(message)) {
      startRead();
    } else {
      unhandled(message);
    }
  }

  private void startRead() {
    LOG.info("start read");
    while (true) {
      ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
      for (ConsumerRecord<String, String> record : records) {
        LOG.info("Received. partition: {} offset: {} key: {} value: {}", record.partition(), record.offset(), record.key(), record.value());
      }
    }
  }

}
