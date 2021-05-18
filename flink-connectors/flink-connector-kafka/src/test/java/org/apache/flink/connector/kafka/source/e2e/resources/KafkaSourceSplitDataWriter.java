package org.apache.flink.connector.kafka.source.e2e.resources;

import org.apache.flink.connectors.test.common.external.SourceSplitDataWriter;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Properties;

/** Source split data writer for writing test data into Kafka topic partitions. */
public class KafkaSourceSplitDataWriter implements SourceSplitDataWriter<String> {

    private final KafkaProducer<byte[], byte[]> kafkaProducer;
    private final TopicPartition topicPartition;

    public KafkaSourceSplitDataWriter(
            Properties producerProperties, TopicPartition topicPartition) {
        this.kafkaProducer = new KafkaProducer<>(producerProperties);
        this.topicPartition = topicPartition;
    }

    @Override
    public void writeRecords(Collection<String> records) {
        for (String record : records) {
            ProducerRecord<byte[], byte[]> producerRecord =
                    new ProducerRecord<>(
                            topicPartition.topic(),
                            topicPartition.partition(),
                            null,
                            record.getBytes(StandardCharsets.UTF_8));
            kafkaProducer.send(producerRecord);
        }
        kafkaProducer.flush();
    }

    @Override
    public void close() {
        kafkaProducer.close();
    }
}
