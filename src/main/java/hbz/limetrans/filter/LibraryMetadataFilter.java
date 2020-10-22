package hbz.limetrans.filter;

import hbz.limetrans.util.FileQueue;
import hbz.limetrans.util.Settings;

import org.metafacture.io.ObjectStdoutWriter;
import org.metafacture.io.ObjectWriter;
import org.metafacture.json.JsonEncoder;
import org.metafacture.metamorph.Filter;
import org.metafacture.metamorph.InlineMorph;
import org.metafacture.metamorph.Metamorph;

import java.io.IOException;

public class LibraryMetadataFilter {

    public static final String DEFAULT_KEY = "001";

    private final FileQueue mInputQueue;
    private final Metamorph mMorphDef;
    private final String mOutputPath;
    private final boolean mPretty;

    public LibraryMetadataFilter(final String aProcessor, final String[] aInput, final String aKey, final String aOperator, final String[][] aFilters, final String aOutput, final boolean aPretty) throws IOException {
        mInputQueue = new FileQueue(aProcessor, true, aInput);

        if (mInputQueue.isEmpty()) {
            throw new IllegalArgumentException("Could not process limetrans filter: no input specified.");
        }

        mMorphDef = buildMorphDef(aKey, aOperator, aFilters);
        mOutputPath = aOutput;
        mPretty = aPretty;
    }

    public LibraryMetadataFilter(final Settings aSettings) throws IOException {
        this(aSettings.get("processor", "MARCXML"),
                aSettings.getAsArray("input"),
                aSettings.get("key", DEFAULT_KEY),
                aSettings.get("operator", "any"),
                new String[][]{aSettings.getAsArray("filter")},
                aSettings.get("output"),
                aSettings.getAsBoolean("pretty", false));
    }

    public void process() {
        final Filter filter = new Filter(mMorphDef);
        final JsonEncoder encoder = new JsonEncoder();
        encoder.setPrettyPrinting(mPretty);

        filter
            .setReceiver(encoder)
            .setReceiver(
                    mOutputPath == null || "-".equals(mOutputPath) ?
                    new ObjectStdoutWriter<String>() :
                    new ObjectWriter<String>(mOutputPath));

        mInputQueue.process(filter);
    }

    /*
     * Filter examples:
     *
     * - "@001": Record with an ID.
     * - "!001": Record with no ID.
     * - "001=ocn958002247": Record with ID "ocn958002247"
     * - "85642.3=Inhaltstext": Record(s) with field "85642.3" equal to "Inhaltstext"
     * - "85642.3=~Inhaltstext": Record(s) with field "85642.3" matching "Inhaltstext"
     * - "Inhaltstext": Record(s) with any field equal to "Inhaltstext"
     * - "~Inhaltstext": Record(s) with any field matching "Inhaltstext"
     */

    public static Metamorph buildMorphDef(final String aKey, final String aOperator, final String[][] aFilters) {
        final String innerOperator = "any".equals(aOperator) ? "all" : "any";

        final InlineMorph metamorph = InlineMorph.in(LibraryMetadataFilter.class)
            .with("<rules>")
            .with("<entity name=\"\" flushWith=\"record\">");

        if (aFilters.length > 0 && aFilters[0].length > 0) {
            metamorph
                .with("<if>")
                .with("<" + aOperator + ">");

            for (final String[] filters : aFilters) {
                if (filters.length > 0) {
                    metamorph
                        .with("<" + innerOperator + ">");

                    for (final String filterParam : filters) {
                        if (filterParam.startsWith("@")) {
                            metamorph
                                .with(data(filterParam.substring(1), true));
                        }
                        else if (filterParam.startsWith("!")) {
                            metamorph
                                .with("<none>")
                                .with(data(filterParam.substring(1), true))
                                .with("</none>");
                        }
                        else {
                            metamorph
                                .with(data(filterParam, false));
                        }
                    }

                    metamorph
                        .with("</" + innerOperator + ">");
                }
            }

            metamorph
                .with("</" + aOperator + ">")
                .with("</if>");
        }

        metamorph
            .with("<data source=\"" + aKey + "\" />")
            .with("</entity>")
            .with("</rules>");

        return metamorph.create();
    }

    private static String data(final String aFilter, final boolean aFilterSource) {
        final String source;
        final String filter;

        final int index = aFilter.indexOf('=');
        if (index != -1) {
            source = aFilter.substring(0, index);
            filter = aFilter.substring(index + 1);
        }
        else if (aFilterSource) {
            source = aFilter;
            filter = null;
        }
        else {
            source = "*";
            filter = aFilter;
        }

        return "<data source=\"" + source + "\">" + (
                filter == null ? "" : filter.startsWith("~") ?
                    "<regexp match=\"" + filter.substring(1) + "\" />" :
                    "<equals string=\"" + filter + "\" />"
                ) + "</data>";
    }

}
