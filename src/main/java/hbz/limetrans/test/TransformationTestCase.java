package hbz.limetrans.test;

import hbz.limetrans.util.FileQueue;
import hbz.limetrans.util.LimetransException;

import org.junit.Assert;
import org.junit.runners.model.Statement;
import org.metafacture.javaintegration.EventList;
import org.metafacture.metamorph.Metamorph;

import java.io.IOException;
import java.util.function.Consumer;

public class TransformationTestCase extends Statement {

    private final String mInput;
    private final String mName;
    private final String mReference;
    private final String mRules;

    public TransformationTestCase(final String aName, final String aReference, final String aInput, final String aRules) {
        mName = aName;
        mReference = aReference;
        mInput = aInput;
        mRules = aRules;
    }

    public String getName() {
        return mName;
    }

    @Override
    public void evaluate() {
        evaluateTransformation(mReference, getEvents(mInput, mRules));
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
        final String ext = aFile.substring(aFile.lastIndexOf('.') + 1);

        final FileQueue inputQueue;
        try {
            inputQueue = new FileQueue("xml".equals(ext) ? "MARCXML" : ext.toUpperCase(), false, aFile);
        }
        catch (final IOException e) {
            throw new LimetransException(e);
        }

        return processEvents(l -> inputQueue.process(l, aRules != null ? new Metamorph(aRules) : null));
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
