package hbz.limetrans.test;

import hbz.limetrans.util.Helpers;

import org.junit.runner.Description;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.ParentRunner;
import org.junit.runners.model.InitializationError;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class TransformationTestRunner extends ParentRunner<TransformationTestCase> {

    public static final String FILE_EXT = ".xml";
    public static final String RULES_PATH = TransformationTestSuite.ROOT_PATH + "/%s.xml";

    private final List<TransformationTestCase> mTestCases;
    private final String mName;

    TransformationTestRunner(final Class aClass, final File aDirectory) throws InitializationError {
        super(aClass);

        mName = aDirectory.getName();
        mTestCases = new ArrayList<>();

        final URL rulesUrl;
        try {
            rulesUrl = Helpers.getResourceUrl(aClass, String.format(RULES_PATH, mName));
        }
        catch (final IOException e) {
            throw new InitializationError(e);
        }

        for (final File file : aDirectory.listFiles()) {
            final String fileName = file.toString();

            if (fileName.endsWith(FILE_EXT)) {
                mTestCases.add(new TransformationTestCase(rulesUrl.getPath(), fileName));
            }
        }

        if (mTestCases.isEmpty()) {
            throw new InitializationError("No test files found: " + aDirectory);
        }
    }

    @Override
    protected String getName() {
        return mName;
    }

    @Override
    protected List<TransformationTestCase> getChildren() {
        return mTestCases;
    }

    @Override
    protected Description describeChild(final TransformationTestCase child) {
        return Description.createTestDescription(getName(), child.getName());
    }

    @Override
    protected void runChild(final TransformationTestCase child, final RunNotifier notifier) {
        runLeaf(child, describeChild(child), notifier);
    }

}
