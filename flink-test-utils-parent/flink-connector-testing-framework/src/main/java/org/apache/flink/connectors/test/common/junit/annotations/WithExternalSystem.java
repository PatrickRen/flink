package org.apache.flink.connectors.test.common.junit.annotations;

import org.apache.flink.connectors.test.common.external.ExternalSystem;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks the field in test class defining {@link ExternalSystem}.
 *
 * <p>Only one field can be annotated as external system in test class.
 *
 * <p>The lifecycle of {@link ExternalSystem} will be PER-CLASS for performance, because launching
 * and tearing down external system could be relatively a heavy operation.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface WithExternalSystem {}
