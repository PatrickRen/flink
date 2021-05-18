package org.apache.flink.connector.kafka.source.e2e;

import org.apache.flink.connector.kafka.source.e2e.resources.KafkaContainerizedExternalSystem;
import org.apache.flink.connector.kafka.source.e2e.resources.KafkaMultipleTopicExternalContext;
import org.apache.flink.connector.kafka.source.e2e.resources.KafkaSingleTopicExternalContext;
import org.apache.flink.connectors.test.common.environment.FlinkContainersTestEnvironment;
import org.apache.flink.connectors.test.common.environment.MiniClusterTestEnvironment;
import org.apache.flink.connectors.test.common.junit.annotations.WithExternalContextFactory;
import org.apache.flink.connectors.test.common.junit.annotations.WithExternalSystem;
import org.apache.flink.connectors.test.common.junit.annotations.WithTestEnvironment;
import org.apache.flink.connectors.test.common.testsuites.TestSuiteBase;
import org.apache.flink.connectors.test.common.utils.TestResourceUtils;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;

/**
 * End-to-end test for {@link org.apache.flink.connector.kafka.source.KafkaSource} using testing
 * framework, based on JUnit 5.
 *
 * <p>This class uses {@link Nested} classes to test functionality of KafkaSource under different
 * Flink environments (MiniCluster and Containers). Each nested class extends {@link TestSuiteBase}
 * for reusing test cases already defined by testing framework.
 */
@DisplayName("Kafka Source E2E Test")
public class KafkaSourceE2ETest {

    @Nested
    @DisplayName("On MiniCluster")
    class OnMiniCluster extends TestSuiteBase<String> {

        // Defines TestEnvironment
        @WithTestEnvironment public MiniClusterTestEnvironment flink;

        // Defines ExternalSystem
        @WithExternalSystem public KafkaContainerizedExternalSystem kafka;

        // Defines 2 External context Factories, so test cases will be invoked twice using these two
        // kinds of external contexts.
        @WithExternalContextFactory public KafkaSingleTopicExternalContext.Factory singleTopic;
        @WithExternalContextFactory public KafkaMultipleTopicExternalContext.Factory multipleTopic;

        /** Instantiate and preparing test resources. */
        public OnMiniCluster() {

            // Flink MiniCluster
            flink = new MiniClusterTestEnvironment();

            // Kafka on container
            kafka = new KafkaContainerizedExternalSystem();

            // The construction of Kafka external context requires bootstrap servers of Kafka, so we
            // start Kafka cluster here to get bootstrap servers for the external context.
            kafka.startUp();
            String bootstrapServers =
                    String.join(
                            ",",
                            kafka.getBootstrapServer(),
                            KafkaContainerizedExternalSystem.ENTRY);

            // External context factories
            singleTopic = new KafkaSingleTopicExternalContext.Factory(bootstrapServers);
            multipleTopic = new KafkaMultipleTopicExternalContext.Factory(bootstrapServers);
        }
    }

    @Nested
    @DisplayName("On Containers")
    class OnContainers extends TestSuiteBase<String> {

        // Defines TestEnvironment
        @WithTestEnvironment public FlinkContainersTestEnvironment flink;

        // Defines ExternalSystem
        @WithExternalSystem public KafkaContainerizedExternalSystem kafka;

        // Defines 2 External context Factories, so test cases will be invoked twice using these two
        // kinds of external contexts.
        @WithExternalContextFactory public KafkaSingleTopicExternalContext.Factory singleTopic;
        @WithExternalContextFactory public KafkaMultipleTopicExternalContext.Factory multipleTopic;

        /** Instantiate and preparing test resources. */
        public OnContainers() throws Exception {

            // Flink on containers
            flink =
                    new FlinkContainersTestEnvironment(
                            1, TestResourceUtils.searchConnectorJar().getAbsolutePath());
            // Kafka on container has to be bound with Flink containers so that they can access each
            // other through internal network, so we have to launch Flink containers here.
            flink.startUp();

            // The construction of Kafka external context requires bootstrap servers of Kafka, so we
            // start Kafka cluster here to get bootstrap servers for the external context.
            kafka =
                    new KafkaContainerizedExternalSystem()
                            .withFlinkContainers(flink.getFlinkContainers());
            kafka.startUp();
            String bootstrapServers =
                    String.join(
                            ",",
                            kafka.getBootstrapServer(),
                            KafkaContainerizedExternalSystem.ENTRY);

            // External context factories
            singleTopic = new KafkaSingleTopicExternalContext.Factory(bootstrapServers);
            multipleTopic = new KafkaMultipleTopicExternalContext.Factory(bootstrapServers);
        }
    }
}
