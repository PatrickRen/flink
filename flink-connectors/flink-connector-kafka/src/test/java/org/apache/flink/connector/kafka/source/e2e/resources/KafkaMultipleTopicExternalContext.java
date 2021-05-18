package org.apache.flink.connector.kafka.source.e2e.resources;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connectors.test.common.external.ExternalContext;
import org.apache.flink.connectors.test.common.external.SourceSplitDataWriter;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;

/**
 * Kafka external context that will create multiple topics with only one partitions as source
 * splits.
 */
public class KafkaMultipleTopicExternalContext extends KafkaSingleTopicExternalContext {

    private int numTopics = 0;

    private static final String TOPIC_PATTERN = "kafka-multiple-topic-.*";

    private final Map<String, SourceSplitDataWriter<String>> topicNameToSplitWriters =
            new HashMap<>();

    public KafkaMultipleTopicExternalContext(String bootstrapServers) {
        super(bootstrapServers);
    }

    @Override
    public SourceSplitDataWriter<String> createSourceSplit() {
        String topicName = getTopicName();
        createTopic(topicName, 1, (short) 1);
        final KafkaSourceSplitDataWriter splitWriter =
                new KafkaSourceSplitDataWriter(
                        getKafkaProducerProperties(numTopics), new TopicPartition(topicName, 0));
        topicNameToSplitWriters.put(topicName, splitWriter);
        numTopics++;
        return splitWriter;
    }

    @Override
    public Source<String, ?, ?> createSource(Boundedness boundedness) {
        KafkaSourceBuilder<String> builder = KafkaSource.builder();

        if (boundedness == Boundedness.BOUNDED) {
            builder = builder.setBounded(OffsetsInitializer.latest());
        }

        return builder.setGroupId("flink-kafka-multiple-topic-test")
                .setBootstrapServers(bootstrapServers)
                .setTopicPattern(Pattern.compile(TOPIC_PATTERN))
                .setDeserializer(
                        KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                .build();
    }

    @Override
    public void close() {
        topicNameToSplitWriters.forEach(
                (topicName, splitWriter) -> {
                    try {
                        splitWriter.close();
                        deleteTopic(topicName);
                    } catch (Exception e) {
                        kafkaAdminClient.close();
                        throw new RuntimeException("Cannot close split writer", e);
                    }
                });
        topicNameToSplitWriters.clear();
        kafkaAdminClient.close();
    }

    private String getTopicName() {
        return TOPIC_PATTERN.replace(".*", String.valueOf(numTopics))
                + "-"
                + ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
    }

    @Override
    public String toString() {
        return "Multiple-topics Kafka";
    }

    /** . */
    public static class Factory implements ExternalContext.Factory<String> {

        private final String bootstrapServers;

        public Factory(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        @Override
        public ExternalContext<String> createExternalContext() {
            return new KafkaMultipleTopicExternalContext(bootstrapServers);
        }
    }
}
