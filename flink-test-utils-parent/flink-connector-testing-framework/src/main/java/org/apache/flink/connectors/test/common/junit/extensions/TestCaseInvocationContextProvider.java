package org.apache.flink.connectors.test.common.junit.extensions;

import org.apache.flink.connectors.test.common.environment.TestEnvironment;
import org.apache.flink.connectors.test.common.external.ExternalContext;
import org.apache.flink.connectors.test.common.junit.annotations.Case;

import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.junit.platform.commons.support.AnnotationSupport;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.flink.connectors.test.common.junit.extensions.TestingFrameworkExtension.EXTERNAL_CONTEXT_FACTORIES_STORE_KEY;
import static org.apache.flink.connectors.test.common.junit.extensions.TestingFrameworkExtension.TEST_ENV_STORE_KEY;
import static org.apache.flink.connectors.test.common.junit.extensions.TestingFrameworkExtension.TEST_RESOURCE_NAMESPACE;

/**
 * A helper class for injecting test resources into test case as parameters.
 *
 * <p>This provider will resolve {@link TestEnvironment} and {@link ExternalContext.Factory} from
 * the storage in JUnit's {@link ExtensionContext} and inject them into test method annotated by
 * {@link Case}, and register a {@link AfterTestExecutionCallback} for closing the external context
 * after the execution of test case.
 */
public class TestCaseInvocationContextProvider implements TestTemplateInvocationContextProvider {

    private static final int TEST_ENVIRONMENT_PARAM_INDEX = 0;
    private static final int EXTERNAL_CONTEXT_PARAM_INDEX = 1;

    @Override
    public boolean supportsTestTemplate(ExtensionContext context) {
        return AnnotationSupport.isAnnotated(context.getRequiredTestMethod(), Case.class);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(
            ExtensionContext context) {
        TestEnvironment testEnv =
                context.getStore(TEST_RESOURCE_NAMESPACE)
                        .get(TEST_ENV_STORE_KEY, TestEnvironment.class);

        List<ExternalContext.Factory<?>> externalContextFactories =
                (List<ExternalContext.Factory<?>>)
                        context.getStore(TEST_RESOURCE_NAMESPACE)
                                .get(EXTERNAL_CONTEXT_FACTORIES_STORE_KEY);

        return externalContextFactories.stream()
                .map(
                        factory ->
                                new TestResourceProvidingInvocationContext(
                                        testEnv, factory.createExternalContext()));
    }

    static class TestResourceProvidingInvocationContext implements TestTemplateInvocationContext {

        private final TestEnvironment testEnvironment;
        private final ExternalContext<?> externalContext;

        public TestResourceProvidingInvocationContext(
                TestEnvironment testEnvironment, ExternalContext<?> externalContext) {
            this.testEnvironment = testEnvironment;
            this.externalContext = externalContext;
        }

        @Override
        public String getDisplayName(int invocationIndex) {
            return String.format(
                    "TestEnvironment: [%s], ExternalContext: [%s]",
                    testEnvironment, externalContext);
        }

        @Override
        public List<Extension> getAdditionalExtensions() {
            return Arrays.asList(
                    new ParameterResolver() {
                        @Override
                        public boolean supportsParameter(
                                ParameterContext parameterContext,
                                ExtensionContext extensionContext)
                                throws ParameterResolutionException {
                            if (parameterContext.getIndex() == TEST_ENVIRONMENT_PARAM_INDEX) {
                                return isAssignableFromParameterType(
                                        TestEnvironment.class,
                                        parameterContext.getParameter().getType());
                            }
                            if (parameterContext.getIndex() == EXTERNAL_CONTEXT_PARAM_INDEX) {
                                return isAssignableFromParameterType(
                                        ExternalContext.class,
                                        parameterContext.getParameter().getType());
                            }
                            return false;
                        }

                        @Override
                        public Object resolveParameter(
                                ParameterContext parameterContext,
                                ExtensionContext extensionContext)
                                throws ParameterResolutionException {
                            if (parameterContext.getIndex() == TEST_ENVIRONMENT_PARAM_INDEX) {
                                return testEnvironment;
                            }
                            if (parameterContext.getIndex() == EXTERNAL_CONTEXT_PARAM_INDEX) {
                                return externalContext;
                            }
                            throw new RuntimeException(
                                    "The test case should have exactly 2 parameters");
                        }
                    },
                    (AfterTestExecutionCallback) ignore -> externalContext.close());
        }

        private boolean isAssignableFromParameterType(
                Class<?> requiredType, Class<?> parameterType) {
            return requiredType.isAssignableFrom(parameterType);
        }
    }
}
