package org.apache.flink.connectors.test.common.junit.extensions;

import org.apache.flink.connectors.test.common.TestResource;
import org.apache.flink.connectors.test.common.environment.TestEnvironment;
import org.apache.flink.connectors.test.common.external.ExternalContext;
import org.apache.flink.connectors.test.common.external.ExternalSystem;
import org.apache.flink.connectors.test.common.junit.annotations.WithExternalContextFactory;
import org.apache.flink.connectors.test.common.junit.annotations.WithExternalSystem;
import org.apache.flink.connectors.test.common.junit.annotations.WithTestEnvironment;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.support.AnnotationSupport;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.List;

/**
 * A JUnit 5 {@link Extension} for supporting running of testing framework.
 *
 * <p>This extension is responsible for searching test resources annotated by {@link
 * WithTestEnvironment}, {@link WithExternalSystem} and {@link WithExternalContextFactory}, storing
 * them into storage provided by JUnit, and manage lifecycle of these resources.
 */
public class TestingFrameworkExtension implements BeforeAllCallback, AfterAllCallback {

    public static final ExtensionContext.Namespace TEST_RESOURCE_NAMESPACE =
            ExtensionContext.Namespace.create("testResourceNamespace");
    public static final String TEST_ENV_STORE_KEY = "testEnvironment";
    public static final String EXTERNAL_SYSTEM_STORE_KEY = "externalSystem";
    public static final String EXTERNAL_CONTEXT_FACTORIES_STORE_KEY = "externalContext";

    private TestEnvironment testEnvironment;
    private ExternalSystem externalSystem;

    @SuppressWarnings("rawtypes")
    @Override
    public void beforeAll(ExtensionContext context) throws Exception {

        final List<TestEnvironment> testEnvironments =
                AnnotationSupport.findAnnotatedFieldValues(
                        context.getRequiredTestInstance(),
                        WithTestEnvironment.class,
                        TestEnvironment.class);
        checkExactlyOneAnnotatedField(testEnvironments, WithTestEnvironment.class);
        testEnvironment = testEnvironments.get(0);
        if (!testEnvironment.isStarted()) {
            testEnvironment.startUp();
        }
        context.getStore(TEST_RESOURCE_NAMESPACE).put(TEST_ENV_STORE_KEY, testEnvironment);

        final List<ExternalSystem> externalSystems =
                AnnotationSupport.findAnnotatedFieldValues(
                        context.getRequiredTestInstance(),
                        WithExternalSystem.class,
                        ExternalSystem.class);
        checkExactlyOneAnnotatedField(externalSystems, WithExternalSystem.class);
        externalSystem = externalSystems.get(0);
        if (!externalSystem.isStarted()) {
            externalSystem.startUp();
        }
        context.getStore(TEST_RESOURCE_NAMESPACE).put(EXTERNAL_SYSTEM_STORE_KEY, externalSystem);

        final List<ExternalContext.Factory> externalContextFactories =
                AnnotationSupport.findAnnotatedFieldValues(
                        context.getRequiredTestInstance(),
                        WithExternalContextFactory.class,
                        ExternalContext.Factory.class);
        checkAtLeastOneAnnotationField(externalContextFactories, WithExternalContextFactory.class);
        context.getStore(TEST_RESOURCE_NAMESPACE)
                .put(EXTERNAL_CONTEXT_FACTORIES_STORE_KEY, externalContextFactories);
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        if (resourceNotNullAndStarted(testEnvironment)) {
            testEnvironment.tearDown();
        }
        if (resourceNotNullAndStarted(externalSystem)) {
            externalSystem.tearDown();
        }
        context.getStore(TEST_RESOURCE_NAMESPACE).remove(TestEnvironment.class.getCanonicalName());
        context.getStore(TEST_RESOURCE_NAMESPACE).remove(ExternalSystem.class.getCanonicalName());
        context.getStore(TEST_RESOURCE_NAMESPACE)
                .remove(ExternalContext.Factory.class.getCanonicalName());
    }

    private void checkExactlyOneAnnotatedField(
            Collection<?> fields, Class<? extends Annotation> annotation) {
        if (fields.size() > 1) {
            throw new IllegalStateException(
                    String.format(
                            "Multiple fields are annotated with '@%s'",
                            annotation.getSimpleName()));
        }
        if (fields.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "No fields are annotated with '@%s'", annotation.getSimpleName()));
        }
    }

    private void checkAtLeastOneAnnotationField(
            Collection<?> fields, Class<? extends Annotation> annotation) {
        if (fields.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "No fields are annotated with '@%s'", annotation.getSimpleName()));
        }
    }

    private boolean resourceNotNullAndStarted(TestResource resource) {
        return resource != null && resource.isStarted();
    }
}
