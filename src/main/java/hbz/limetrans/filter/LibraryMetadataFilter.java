package hbz.limetrans.filter;

import hbz.limetrans.util.FileQueue;

import org.culturegraph.mf.morph.InlineMorph;
import org.culturegraph.mf.morph.Metamorph;
import org.culturegraph.mf.stream.converter.JsonEncoder;
import org.culturegraph.mf.stream.converter.xml.MarcXmlHandler;
import org.culturegraph.mf.stream.converter.xml.XmlDecoder;
import org.culturegraph.mf.stream.pipe.Filter;
import org.culturegraph.mf.stream.pipe.StreamUnicodeNormalizer;
import org.culturegraph.mf.stream.sink.ObjectStdoutWriter;
import org.culturegraph.mf.stream.sink.ObjectWriter;
import org.culturegraph.mf.stream.source.FileOpener;
import org.xbib.common.settings.Settings;

import java.io.IOException;

public class LibraryMetadataFilter {

    private final FileQueue mInputQueue;
    private final Metamorph mMorphDef;
    private final String mOutputPath;

    public LibraryMetadataFilter(final Settings aSettings) throws IOException {
        mInputQueue = new FileQueue(aSettings.getAsArray("input"));
        if (mInputQueue.isEmpty()) {
            throw new IllegalArgumentException("Could not process limetrans filter: no input specified.");
        }

        mOutputPath = aSettings.get("output");
        mMorphDef = buildMorphDef(aSettings);
    }

    public void process() {
        final FileOpener opener = new FileOpener();
        final XmlDecoder decoder = new XmlDecoder();
        final MarcXmlHandler marcHandler = new MarcXmlHandler();
        final StreamUnicodeNormalizer normalizer = new StreamUnicodeNormalizer();
        final Filter filter = new Filter(mMorphDef);
        final JsonEncoder encoder = new JsonEncoder();

        encoder.setPrettyPrinting(true);

        opener
            .setReceiver(decoder)
            .setReceiver(marcHandler)
            .setReceiver(normalizer)
            .setReceiver(filter)
            .setReceiver(encoder)
            .setReceiver(
                    (mOutputPath == null || mOutputPath.equals("-")) ?
                    new ObjectStdoutWriter<String>() :
                    new ObjectWriter<String>(mOutputPath));

        mInputQueue.process(opener);

        opener.closeStream();
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
        final InlineMorph morph = InlineMorph.in(this)
            .with("<rules>")
            .with("<entity name=\"\" flushWith=\"record\">");

        final String[] filters = aSettings.getAsArray("filter");
        if (filters.length > 0) {
            morph.with("<if>");

            for (String filter : filters) {
                String source = "*";

                final int index = filter.indexOf("=");
                if (index != -1) {
                    source = filter.substring(0, index);
                    filter = filter.substring(index + 1);
                }

                morph
                    .with("<data source=\"" + source + "\">")
                    .with(filter.startsWith("~") ?
                            "<regexp match=\"" + filter.substring(1) + "\" />" :
                            "<equals string=\"" + filter + "\" />")
                    .with("</data>")
            }

            morph.with("</if>");
        }

        return morph
            .with("<data source=\"001\" />")
            .with("</entity>")
            .with("</rules>")
            .create();
    }

}
