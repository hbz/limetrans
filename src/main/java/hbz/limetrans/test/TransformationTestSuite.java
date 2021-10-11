package hbz.limetrans.test;

import hbz.limetrans.Limetrans;
import hbz.limetrans.util.Helpers;

import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.ParentRunner;
import org.junit.runners.model.InitializationError;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TransformationTestSuite extends ParentRunner<Runner> {

    public static final String ROOT_PATH = "/transformation";

    private final List<Runner> mRunners;

    public TransformationTestSuite(final Class<?> aClass, final Limetrans.Type aType) throws InitializationError {
        super(aClass);

        mRunners = new ArrayList<>();

        final String root;
        try {
            root = Helpers.getResourcePath(aClass, ROOT_PATH);
        }
        catch (final IOException e) {
            throw new InitializationError(e);
        }

        for (final File directory : new File(root).listFiles()) {
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

}
