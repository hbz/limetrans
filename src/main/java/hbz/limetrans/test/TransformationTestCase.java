package hbz.limetrans.test;

import hbz.limetrans.Limetrans;
import hbz.limetrans.util.FileQueue;
import hbz.limetrans.util.Helpers;
import hbz.limetrans.util.LimetransException;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.runners.model.Statement;
import org.metafacture.framework.StreamReceiver;
import org.metafacture.io.ObjectWriter;
import org.metafacture.javaintegration.EventList;
import org.metafacture.json.JsonEncoder;

import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;

public class TransformationTestCase extends Statement {

    private final String mInput;
    private final String mName;
    private final String mReference;
    private final String mRules;
    private final boolean mRequired;

    public TransformationTestCase(final String aName, final String aReference, final String aInput, final String aRules, final boolean aRequired) {
        mName = aName;
        mReference = aReference;
        mInput = aInput;
        mRules = aRules;
        mRequired = aRequired;
    }

    public String getName() {
        return mName;
    }

    @Override
    public void evaluate() {
        try {
            Helpers.updateTestFile(mReference, () -> {
                try {
                    final File outputFile = File.createTempFile(new File(mReference).getName(), "");
                    outputFile.deleteOnExit();

                    final JsonEncoder jsonEncoder = new JsonEncoder();
                    jsonEncoder.setPrettyPrinting(true);
                    jsonEncoder.setReceiver(new ObjectWriter<>(outputFile.getPath()));

                    processFile(mInput, mRules, jsonEncoder);

                    return outputFile.getPath();
                }
                catch (final IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        catch (final IOException e) {
            throw new RuntimeException(e);
        }

        if (mReference != null && new File(mReference).exists()) {
            evaluateTransformation(mReference, getEvents(mInput, mRules));
        }
        else {
            Assume.assumeTrue(mRequired);
        }
    }

    public static void evaluateTransformation(final String aReference, final Consumer<EventList> aConsumer) {
        evaluateTransformation(aReference, processEvents(aConsumer));
    }

    private static void evaluateTransformation(final String aReference, final EventStack aActual) {
        final EventStack expected = getEvents(aReference, null);

        evaluateCommon(expected, aActual);

        evaluateRemaining(expected, "Missing events");
        evaluateRemaining(aActual, "Unexpected events");
    }

    private static EventStack getEvents(final String aFile, final String aRules) {
        return processEvents(l -> processFile(aFile, aRules, l));
    }

    private static void processFile(final String aFile, final String aRules, final StreamReceiver aReceiver) {
        final String ext = aFile.substring(aFile.lastIndexOf('.') + 1);

        final FileQueue inputQueue;
        try {
            inputQueue = new FileQueue("xml".equals(ext) ? "MARCXML" : ext.toUpperCase(), false, aFile);
        }
        catch (final IOException e) {
            throw new LimetransException(e);
        }

        inputQueue.process(aReceiver, Limetrans.getStreamPipe(aRules, null, null)).closeStream();
    }

    private static EventStack processEvents(final Consumer<EventList> aConsumer) {
        final EventList eventList = new EventList();
        aConsumer.accept(eventList);

        return new EventStack(eventList);
    }

    private static void evaluateCommon(final EventStack aExpected, final EventStack aActual) {
        while (aExpected.hasNext() && aActual.hasNext()) {
            final EventStackEntry expected = aExpected.next();
            final EventStackEntry actual = aActual.next();

            final EventStackEntry.Mismatch mismatch = expected.getMismatch(actual);

            if (mismatch != null) {
                Assert.fail(new StringBuilder()
                        .append(mismatch)
                        .append("\nexpected:\n\t")
                        .append(expected)
                        .append("\nbut was:\n\t")
                        .append(actual)
                        .toString());
            }
        }
    }

    private static void evaluateRemaining(final EventStack aStack, final String aMsg) {
        if (aStack.hasNext()) {
            final StringBuilder builder = new StringBuilder(aMsg);

            while (aStack.hasNext()) {
                builder
                    .append("\n\t")
                    .append(aStack.next().getEvent());
            }

            Assert.fail(builder.toString());
        }
    }

}
