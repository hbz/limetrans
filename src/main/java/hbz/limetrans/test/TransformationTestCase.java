package hbz.limetrans.test;

import org.culturegraph.mf.biblio.marc21.MarcXmlHandler;
import org.culturegraph.mf.commons.ResourceUtil;
import org.culturegraph.mf.formeta.FormetaDecoder;
import org.culturegraph.mf.formeta.FormetaRecordsReader;
import org.culturegraph.mf.javaintegration.EventList.Event;
import org.culturegraph.mf.javaintegration.EventList;
import org.culturegraph.mf.metamorph.Metamorph;
import org.culturegraph.mf.xml.XmlDecoder;

import org.junit.Assert;
import org.junit.runners.model.Statement;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Reader;
import java.util.Iterator;
import java.util.Stack;

public class TransformationTestCase extends Statement {

    private final String mFileName;
    private final String mRulesPath;

    TransformationTestCase(final String aRulesPath, final String aFileName) {
        mRulesPath = aRulesPath;
        mFileName = aFileName.substring(0, aFileName.lastIndexOf("."));
    }

    public String getName() {
        return mFileName.substring(mFileName.lastIndexOf(File.separator) + 1);
    }

    @Override
    public void evaluate() {
        validate(getExpected(), getActual());
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

    private void validate(final EventList aExpected, final EventList aActual) {
        final Iterator<Event> expectedEvents = aExpected.getEvents().iterator();
        final Iterator<Event> actualEvents = aActual.getEvents().iterator();

        final Stack<String> entityStack = new Stack<>();
        int recordNumber = 0;

        while (expectedEvents.hasNext() && actualEvents.hasNext()) {
            final Event expected = expectedEvents.next();
            final Event actual = actualEvents.next();

            switch (expected.getType()) {
                case START_RECORD:
                    entityStack.push("<" + (++recordNumber) + ">");
                    break;
                case START_ENTITY:
                    entityStack.push(expected.getName());
                    break;
                case END_ENTITY:
                    entityStack.pop();
                    break;
                case END_RECORD:
                    entityStack.clear();
                    break;
            }

            Assert.assertEquals(String.join("/", entityStack),
                    expected.toString(), actual.toString());
        }

        Assert.assertFalse("Missing events", expectedEvents.hasNext());
        Assert.assertFalse("Unexpected events", actualEvents.hasNext());
    }

    private Reader getData(final String aExt) {
        try {
            return ResourceUtil.getReader(mFileName + "." + aExt);
        }
        catch (final FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

}
