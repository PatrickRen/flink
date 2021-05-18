package org.apache.flink.connectors.test.common.external;

import java.util.Collection;

/**
 * A data writer for writing records into a {@link
 * org.apache.flink.api.connector.source.SourceSplit} in the external system.
 *
 * @param <T> Type of writing records
 */
public interface SourceSplitDataWriter<T> extends AutoCloseable {
    void writeRecords(Collection<T> records);
}
