package hbz.limetrans.filter;

import hbz.limetrans.util.FileQueue;

import org.metafacture.io.ObjectStdoutWriter;
import org.metafacture.io.ObjectWriter;
import org.metafacture.json.JsonEncoder;
import org.metafacture.metamorph.Filter;
import org.metafacture.metamorph.InlineMorph;
import org.metafacture.metamorph.Metamorph;
import org.xbib.common.settings.Settings;

import java.io.IOException;

public class LibraryMetadataFilter {

    private final FileQueue mInputQueue;
    private final Metamorph mMorphDef;
    private final String mOutputPath;

    public LibraryMetadataFilter(final Settings aSettings) throws IOException {
        mInputQueue = new FileQueue("MARCXML", aSettings.getAsArray("input"));

        if (mInputQueue.isEmpty()) {
            throw new IllegalArgumentException("Could not process limetrans filter: no input specified.");
        }

        mOutputPath = aSettings.get("output");
        mMorphDef = buildMorphDef(aSettings);
    }

    public void process() {
        final Filter filter = new Filter(mMorphDef);
        final JsonEncoder encoder = new JsonEncoder();
        encoder.setPrettyPrinting(true);

        filter
            .setReceiver(encoder)
            .setReceiver(
                    mOutputPath == null || "-".equals(mOutputPath) ?
                    new ObjectStdoutWriter<String>() :
                    new ObjectWriter<String>(mOutputPath));

        mInputQueue.process(filter, true);
    }

    /*
     * Filter examples:
     *
     * - "001=ocn958002247": Record with ID "ocn958002247"
     * - "85642.3=Inhaltstext": Record(s) with field "85642.3" equal to "Inhaltstext"
     * - "85642.3=~Inhaltstext": Record(s) with field "85642.3" matching "Inhaltstext"
     * - "Inhaltstext": Record(s) with any field equal to "Inhaltstext"
     * - "~Inhaltstext": Record(s) with any field matching "Inhaltstext"
     */

    private Metamorph buildMorphDef(final Settings aSettings) {
        final InlineMorph metamorph = InlineMorph.in(this)
            .with("<rules>")
            .with("<entity name=\"\" flushWith=\"record\">");

        final String[] filters = aSettings.getAsArray("filter");
        if (filters.length > 0) {
            metamorph.with("<if>");

            for (final String filterParam : filters) {
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

            metamorph.with("</if>");
        }

        return metamorph
            .with("<data source=\"001\" />")
            .with("</entity>")
            .with("</rules>")
            .create();
    }

}
