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

    private final FileQueue mInputQueue;
    private final Metamorph mMorphDef;
    private final String mOutputPath;
    private final boolean mPretty;

    public LibraryMetadataFilter(final Settings aSettings) throws IOException {
        mInputQueue = new FileQueue(aSettings.get("processor", "MARCXML"), true, aSettings.getAsArray("input"));

        if (mInputQueue.isEmpty()) {
            throw new IllegalArgumentException("Could not process limetrans filter: no input specified.");
        }

        mMorphDef = buildMorphDef(aSettings.get("operator", "any"), aSettings.getAsArray("filter"));
        mOutputPath = aSettings.get("output");
        mPretty = aSettings.getAsBoolean("pretty", false);
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

    public static Metamorph buildMorphDef(final String aOperator, final String... aFilters) {
        final InlineMorph metamorph = InlineMorph.in(LibraryMetadataFilter.class)
            .with("<rules>")
            .with("<entity name=\"\" flushWith=\"record\">");

        if (aFilters.length > 0) {
            metamorph.with("<if>");
            metamorph.with("<" + aOperator + ">");

            for (final String filterParam : aFilters) {
                if (filterParam.startsWith("@")) {
                    metamorph
                        .with("<data source=\"" + filterParam.substring(1) + "\" />");
                }
                else if (filterParam.startsWith("!")) {
                    metamorph
                        .with("<none>")
                        .with("<data source=\"" + filterParam.substring(1) + "\" />")
                        .with("</none>");
                }
                else {
                    final String source;
                    final String filter;

                    final int index = filterParam.indexOf('=');
                    if (index != -1) {
                        source = filterParam.substring(0, index);
                        filter = filterParam.substring(index + 1);
                    }
                    else {
                        source = "*";
                        filter = filterParam;
                    }

                    metamorph
                        .with("<data source=\"" + source + "\">")
                        .with(filter.startsWith("~") ?
                                "<regexp match=\"" + filter.substring(1) + "\" />" :
                                "<equals string=\"" + filter + "\" />")
                        .with("</data>");
                }
            }

            metamorph.with("</" + aOperator + ">");
            metamorph.with("</if>");
        }

        metamorph
            .with("<data source=\"001\" />")
            .with("</entity>")
            .with("</rules>");

        return metamorph.create();
    }

}
