package hbz.limetrans.test;

import hbz.limetrans.util.Helpers;

import org.junit.runner.Description;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.ParentRunner;
import org.junit.runners.model.InitializationError;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransformationTestRunner extends ParentRunner<TransformationTestCase> {

    public static final String RULES_PATH = TransformationTestSuite.ROOT_PATH + "/%s.xml";

    private final List<TransformationTestCase> mTestCases;
    private final String mName;

    public TransformationTestRunner(final Class<?> aClass, final File aDirectory) throws InitializationError {
        super(aClass);

        mName = aDirectory.getName();
        mTestCases = new ArrayList<>();

        final String rules;
        try {
            rules = Helpers.getResourceUrl(aClass, String.format(RULES_PATH, mName)).getPath();
        }
        catch (final IOException e) {
            throw new InitializationError(e);
        }

        final Map<String, String> referenceFiles = listFiles(aDirectory, "reference");
        final Map<String, String> inputFiles = listFiles(aDirectory, "input");

        for (final Map.Entry<String, String> entry : referenceFiles.entrySet()) {
            final String base = entry.getKey();
            final String path = entry.getValue();

            if (inputFiles.containsKey(base)) {
                mTestCases.add(new TransformationTestCase(base, path, inputFiles.get(base), rules));
            }
            else {
                throw new InitializationError("Missing input file: " + path);
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

    private Map<String, String> listFiles(final File aDir, final String aName) {
        final Map<String, String> list = new HashMap<>();

        for (final File file : aDir.toPath().resolve(aName).toFile().listFiles()) {
            list.put(file.getName().substring(0, file.getName().lastIndexOf('.')), file.getPath());
        }

        return list;
    }

}
