package org.apache.flink.connectors.test.common.junit.annotations;

import org.apache.flink.connectors.test.common.environment.TestEnvironment;
import org.apache.flink.connectors.test.common.external.ExternalContext;
import org.apache.flink.connectors.test.common.junit.extensions.TestCaseInvocationContextProvider;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a test case in the testing framework.
 *
 * <p>A test case in testing framework is a specialization of a {@link TestTemplate} in JUnit 5,
 * which will inject 2 parameters in the test case: {@link TestEnvironment} and {@link
 * ExternalContext} with the help of {@link TestCaseInvocationContextProvider}.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@TestTemplate
@ExtendWith(TestCaseInvocationContextProvider.class)
public @interface Case {}
