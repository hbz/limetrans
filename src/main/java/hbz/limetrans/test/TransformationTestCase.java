package hbz.limetrans.test;

import org.culturegraph.mf.commons.ResourceUtil;
import org.culturegraph.mf.javaintegration.EventList;
import org.culturegraph.mf.metamorph.Metamorph;
import org.culturegraph.mf.test.reader.FormetaReader;
import org.culturegraph.mf.test.reader.MarcXmlReader;
import org.culturegraph.mf.test.reader.Reader;
import org.culturegraph.mf.test.validators.StreamValidator;

import org.junit.runners.model.Statement;

import java.io.File;
import java.io.FileNotFoundException;

public class TransformationTestCase extends Statement {

    private final String mFileName;
    private final String mRulesPath;

    private boolean mStrictKeyOrder = false;
    private boolean mStrictRecordOrder = false;
    private boolean mStrictValueOrder = false;

    TransformationTestCase(final String aRulesPath, final String aFileName) {
        mRulesPath = aRulesPath;
        mFileName = aFileName.substring(0, aFileName.lastIndexOf("."));
    }

    public String getName() {
        return mFileName.substring(mFileName.lastIndexOf(File.separator) + 1);
    }

    public void setStrictRecordOrder(final boolean aValue) {
        mStrictRecordOrder = aValue;
    }

    public void setStrictKeyOrder(final boolean aValue) {
        mStrictKeyOrder = aValue;
    }

    public void setStrictValueOrder(final boolean aValue) {
        mStrictValueOrder = aValue;
    }

    @Override
    public void evaluate() {
        final Reader inputReader = new MarcXmlReader();
        final Reader resultReader = new FormetaReader();
        final EventList resultStream = new EventList();

        inputReader
            .setReceiver(new Metamorph(mRulesPath))
            .setReceiver(resultStream);

        inputReader.process(getData("xml"));
        inputReader.closeStream();

        resultReader
            .setReceiver(getValidator(resultStream));

        resultReader.process(getData("formeta"));
        resultReader.closeStream();
    }

    private java.io.Reader getData(final String aExt) {
        try {
            return ResourceUtil.getReader(mFileName + "." + aExt);
        }
        catch (final FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private StreamValidator getValidator(final EventList aResultStream) {
        final StreamValidator validator = new StreamValidator(aResultStream.getEvents());

        validator.setErrorHandler(msg -> { throw new AssertionError(msg); });
        validator.setStrictRecordOrder(mStrictRecordOrder);
        validator.setStrictKeyOrder(mStrictKeyOrder);
        validator.setStrictValueOrder(mStrictValueOrder);

        return validator;
    }

}
