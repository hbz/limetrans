package hbz.limetrans.test;

import hbz.limetrans.Limetrans;

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

    public TransformationTestSuite(final Class<?> aClass, final Limetrans.Type aType) throws InitializationError {
        super(aClass);

        mRunners = new ArrayList<>();

        final File root = new File(ROOT_PATH);
        if (!root.exists()) {
            throw new InitializationError("Root path not found: " + ROOT_PATH);
        }

        for (final File directory : root.listFiles()) {
            mRunners.add(new TransformationTestRunner(aClass, directory, aType));
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

    public static class Metamorph extends TransformationTestSuite {

        public Metamorph(final Class<?> aClass) throws InitializationError {
            super(aClass, Limetrans.Type.METAMORPH);
        }

    }

    public static class Metafix extends TransformationTestSuite {

        public Metafix(final Class<?> aClass) throws InitializationError {
            super(aClass, Limetrans.Type.METAFIX);
        }

    }

}
