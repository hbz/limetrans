package hbz.limetrans.test;

import org.junit.runner.Description;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.ParentRunner;
import org.junit.runners.model.InitializationError;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class TransformationTestRunner extends ParentRunner<TransformationTestCase> {

    public static final String FILE_EXT = ".xml";
    public static final String RULES_PATH = "src/main/resources/transformation/%s.xml";

    private final List<TransformationTestCase> mTestCases;
    private final String mName;
    private final String mRulesPath;

    TransformationTestRunner(final Class<?> aClass, final File aDirectory) throws InitializationError {
        super(aClass);

        mName = aDirectory.getName();
        mRulesPath = String.format(RULES_PATH, mName);

        mTestCases = new ArrayList<>();

        for (final File file : aDirectory.listFiles()) {
            final String fileName = file.toString();

            if (fileName.endsWith(FILE_EXT)) {
                mTestCases.add(new TransformationTestCase(mRulesPath, fileName));
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
