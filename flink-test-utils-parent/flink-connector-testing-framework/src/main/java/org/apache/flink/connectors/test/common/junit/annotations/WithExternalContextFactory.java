package org.apache.flink.connectors.test.common.junit.annotations;

import org.apache.flink.connectors.test.common.external.ExternalContext;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks the field in test class defining a {@link ExternalContext.Factory} for constructing {@link
 * ExternalContext} before invocation of each test case.
 *
 * <p>Multiple fields can be annotated as external context factory, and these external contexts will
 * be provided as different parameters of test cases.
 *
 * <p>The lifecycle of a {@link ExternalContext} will be PER-CASE, which means an instance of {@link
 * ExternalContext} will be constructed before invocation of each test case, and closed right after
 * the execution of the case for isolation between test cases.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface WithExternalContextFactory {}
