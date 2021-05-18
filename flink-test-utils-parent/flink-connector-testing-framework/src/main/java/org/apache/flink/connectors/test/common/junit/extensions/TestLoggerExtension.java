package org.apache.flink.connectors.test.common.junit.extensions;

import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;

/** A JUnit-5-style test logger, similar to {@link org.apache.flink.util.TestLogger}. */
public class TestLoggerExtension implements TestWatcher, BeforeEachCallback {
    private static final Logger LOG = LoggerFactory.getLogger(TestLoggerExtension.class);

    @Override
    public void beforeEach(ExtensionContext context) {
        LOG.info(
                "\n================================================================================"
                        + "\n🧪 Test {}.{} is running."
                        + "\n--------------------------------------------------------------------------------",
                context.getRequiredTestClass().getCanonicalName(),
                context.getRequiredTestMethod().getName());
    }

    @Override
    public void testSuccessful(ExtensionContext context) {
        LOG.info(
                "\n--------------------------------------------------------------------------------"
                        + "\nTest {}.{} successfully run."
                        + "\n================================================================================",
                context.getRequiredTestClass().getCanonicalName(),
                context.getRequiredTestMethod().getName());
    }

    @Override
    public void testFailed(ExtensionContext context, Throwable cause) {
        LOG.error(
                "\n--------------------------------------------------------------------------------"
                        + "\nTest {}.{} failed with:\n{}"
                        + "\n================================================================================",
                context.getRequiredTestClass().getCanonicalName(),
                context.getRequiredTestMethod().getName(),
                exceptionToString(cause));
    }

    private static String exceptionToString(Throwable t) {
        if (t == null) {
            return "(null)";
        }

        try {
            StringWriter stm = new StringWriter();
            PrintWriter wrt = new PrintWriter(stm);
            t.printStackTrace(wrt);
            wrt.close();
            return stm.toString();
        } catch (Throwable ignored) {
            return t.getClass().getName() + " (error while printing stack trace)";
        }
    }
}
