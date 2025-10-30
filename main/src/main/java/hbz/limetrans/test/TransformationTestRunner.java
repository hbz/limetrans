package hbz.limetrans.test;

import hbz.limetrans.Limetrans;

import org.junit.runner.Description;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.ParentRunner;
import org.junit.runners.model.InitializationError;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransformationTestRunner extends ParentRunner<TransformationTestCase> {

    public static final String RULES_PATH = TransformationTestSuite.ROOT_PATH.replace("test", "main") + "/%s%s";

    private final List<TransformationTestCase> mTestCases;
    private final String mName;

    public TransformationTestRunner(final Class<?> aClass, final File aDirectory, final Limetrans.Type aType) throws InitializationError {
        super(aClass);

        final String name = aDirectory.getName();
        mName = aClass.getName() + "." + name;

        mTestCases = new ArrayList<>();

        final String rules = String.format(RULES_PATH, name, aType.getExtension());

        if (!new File(rules).exists()) {
            if (aType.getRequired()) {
                throw new InitializationError("Rules file not found: " + rules);
            }
            else {
                mTestCases.add(new TransformationTestCase(null, null, null, null, false));
                return;
            }
        }

        final Map<String, String> referenceFiles = listFiles(aDirectory, "reference" + aType.getExtension());
        final Map<String, String> inputFiles = listFiles(aDirectory, "input");

        for (final Map.Entry<String, String> entry : referenceFiles.entrySet()) {
            final String base = entry.getKey();
            final String path = entry.getValue();

            if (inputFiles.containsKey(base)) {
                mTestCases.add(new TransformationTestCase(base, path, inputFiles.get(base), rules, true));
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
        final File[] files = aDir.toPath().resolve(aName).toFile().listFiles();

        if (files != null) {
            for (final File file : files) {
                list.put(file.getName().substring(0, file.getName().lastIndexOf('.')), file.getPath());
            }
        }

        return list;
    }

}
