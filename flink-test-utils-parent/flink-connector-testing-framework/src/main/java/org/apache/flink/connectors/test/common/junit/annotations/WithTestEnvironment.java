package org.apache.flink.connectors.test.common.junit.annotations;

import org.apache.flink.connectors.test.common.environment.TestEnvironment;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks the field in test class defining {@link TestEnvironment}.
 *
 * <p>Only one field can be annotated as test environment in a test class.
 *
 * <p>The lifecycle of {@link TestEnvironment} will be PER-CLASS for performance, because launching
 * and tearing down Flink cluster could be relatively a heavy operation.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface WithTestEnvironment {}
