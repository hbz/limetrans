package hbz.limetrans.test;

import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.ParentRunner;
import org.junit.runners.model.InitializationError;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class TransformationTestSuite extends ParentRunner<Runner> {

    public static final String ROOT_PATH = "src/test/resources/transformation";

    private final List<Runner> mRunners;

    public TransformationTestSuite(final Class<?> aClass) throws InitializationError {
        super(aClass);

        mRunners = new ArrayList<>();

        for (final File directory : new File(ROOT_PATH).listFiles()) {
            mRunners.add(new TransformationTestRunner(aClass, directory));
        }

        if (mRunners.isEmpty()) {
            throw new InitializationError("No test directories found: " + ROOT_PATH);
        }
    }

    @Override
    protected List<Runner> getChildren() {
        return mRunners;
    }

    @Override
    protected Description describeChild(final Runner child) {
        return child.getDescription();
    }

    @Override
    protected void runChild(final Runner child, final RunNotifier notifier) {
        child.run(notifier);
    }

}
