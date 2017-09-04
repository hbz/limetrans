package hbz.limetrans.test;

import hbz.limetrans.util.FileQueue;
import hbz.limetrans.util.LimetransException;

import org.culturegraph.mf.javaintegration.EventList;
import org.culturegraph.mf.metamorph.Metamorph;

import org.junit.Assert;
import org.junit.runners.model.Statement;

import java.io.IOException;

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
        final EventStack expected = getEvents(mReference, null);
        final EventStack actual = getEvents(mInput, mRules);

        evaluateCommon(expected, actual);

        evaluateRemaining(expected, "Missing events");
        evaluateRemaining(actual, "Unexpected events");
    }

    private EventStack getEvents(final String aFile, final String aRules) {
        final String ext = aFile.substring(aFile.lastIndexOf('.') + 1);

        final FileQueue inputQueue;
        try {
            inputQueue = new FileQueue(ext.equals("xml") ? "MARCXML" : ext.toUpperCase(), aFile);
        }
        catch (final IOException e) {
            throw new LimetransException(e);
        }

        final EventList eventList = new EventList();
        inputQueue.process(eventList, aRules != null ? new Metamorph(aRules) : null);

        return new EventStack(eventList);
    }

    private void evaluateCommon(final EventStack aExpected, final EventStack aActual) {
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

    private void evaluateRemaining(final EventStack aStack, final String aMsg) {
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
