package hbz.limetrans.test;

import hbz.limetrans.util.LimetransException;

import org.culturegraph.mf.biblio.marc21.MarcXmlHandler;
import org.culturegraph.mf.commons.ResourceUtil;
import org.culturegraph.mf.formeta.FormetaDecoder;
import org.culturegraph.mf.formeta.FormetaRecordsReader;
import org.culturegraph.mf.javaintegration.EventList;
import org.culturegraph.mf.metamorph.Metamorph;
import org.culturegraph.mf.xml.XmlDecoder;

import org.junit.Assert;
import org.junit.runners.model.Statement;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Reader;

public class TransformationTestCase extends Statement {

    private final String mFileName;
    private final String mRulesPath;

    public TransformationTestCase(final String aRulesPath, final String aFileName) {
        mRulesPath = aRulesPath;
        mFileName = aFileName.substring(0, aFileName.lastIndexOf('.'));
    }

    public String getName() {
        return mFileName.substring(mFileName.lastIndexOf(File.separator) + 1);
    }

    @Override
    public void evaluate() {
        final EventStack expected = new EventStack(getExpected());
        final EventStack actual = new EventStack(getActual());

        evaluateCommon(expected, actual);

        evaluateRemaining(expected, "Missing events");
        evaluateRemaining(actual, "Unexpected events");
    }

    private EventList getExpected() {
        final EventList eventList = new EventList();
        final FormetaRecordsReader reader = new FormetaRecordsReader();

        reader
            .setReceiver(new FormetaDecoder())
            .setReceiver(eventList);

        reader.process(getData("formeta"));
        reader.closeStream();

        return eventList;
    }

    private EventList getActual() {
        final EventList eventList = new EventList();
        final XmlDecoder reader = new XmlDecoder();

        reader
            .setReceiver(new MarcXmlHandler())
            .setReceiver(new Metamorph(mRulesPath))
            .setReceiver(eventList);

        reader.process(getData("xml"));
        reader.closeStream();

        return eventList;
    }

    private Reader getData(final String aExt) {
        try {
            return ResourceUtil.getReader(mFileName + "." + aExt);
        }
        catch (final FileNotFoundException e) {
            throw new LimetransException(e);
        }
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
