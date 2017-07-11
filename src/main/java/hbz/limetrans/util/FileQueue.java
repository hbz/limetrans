package hbz.limetrans.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.culturegraph.mf.biblio.marc21.Marc21Decoder;
import org.culturegraph.mf.biblio.marc21.MarcXmlHandler;
import org.culturegraph.mf.framework.Sender;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.io.FileOpener;
import org.culturegraph.mf.io.LineReader;
import org.culturegraph.mf.strings.StreamUnicodeNormalizer;
import org.culturegraph.mf.xml.XmlDecoder;
import org.xbib.common.settings.Settings;
import org.xbib.util.Finder.PathFile;
import org.xbib.util.Finder;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class FileQueue implements Iterable<String> {

    private enum Processor {
        MARCXML {
            @Override
            public Sender<StreamReceiver> process(final FileOpener aOpener) {
                return aOpener
                    .setReceiver(new XmlDecoder())
                    .setReceiver(new MarcXmlHandler());
            }
        },

        MARC21 {
            @Override
            public Sender<StreamReceiver> process(final FileOpener aOpener) {
                return aOpener
                    .setReceiver(new LineReader())
                    .setReceiver(new Marc21Decoder());
            }
        };

        public abstract Sender<StreamReceiver> process(FileOpener aOpener);

        public Sender<StreamReceiver> process(final FileOpener aOpener, final boolean aNormalizeUnicode) {
            final Sender<StreamReceiver> sender = process(aOpener);
            return aNormalizeUnicode ? sender.setReceiver(new StreamUnicodeNormalizer()) : sender;
        }
    }

    private static final Processor DEFAULT_PROCESSOR = Processor.MARCXML;

    private static final Logger mLogger = LogManager.getLogger();

    private final Queue<String> mQueue = new LinkedList<>();
    private final Processor mProcessor;

    public FileQueue(final Settings aSettings) throws IOException {
        final String processor = aSettings == null ? null : aSettings.get("processor");
        mProcessor = processor == null ? DEFAULT_PROCESSOR : Processor.valueOf(processor);

        add(aSettings);
    }

    public FileQueue(final String[] aFileNames) throws IOException {
        this(aFileNames, DEFAULT_PROCESSOR);
    }

    public FileQueue(final String[] aFileNames, final Processor aProcessor) throws IOException {
        mProcessor = aProcessor;

        for (final String fileName : aFileNames) {
            final File file = new File(fileName);

            add(Settings.settingsBuilder()
                    .put("path", file.getParent())
                    .put("pattern", file.getName())
                    .build());
        }
    }

    @Override
    public Iterator<String> iterator() {
        return mQueue.iterator();
    }

    public void process(final StreamReceiver aReceiver) {
        process(aReceiver, true);
    }

    public void process(final StreamReceiver aReceiver, final boolean aNormalizeUnicode) {
        final FileOpener opener = new FileOpener();

        mProcessor.process(opener, aNormalizeUnicode).setReceiver(aReceiver);

        for (final String fileName : this) {
            mLogger.info("Processing {} file: {}", mProcessor, fileName);
            opener.process(fileName);
        }

        mLogger.info("Finished processing {} files", mProcessor);

        opener.closeStream();
    }

    public boolean isEmpty() {
        return mQueue.isEmpty();
    }

    public int size() {
        return mQueue.size();
    }

    private void add(final Settings aSettings) throws IOException {
        if (aSettings == null) {
            return;
        }

        mLogger.debug("Settings: {}", aSettings.getAsMap());

        if (aSettings.containsSetting("patterns")) {
            for (final String pattern : aSettings.getAsArray("patterns")) {
                add(aSettings, pattern);
            }
        }
        else {
            add(aSettings, aSettings.get("pattern"));
        }
    }

    private void add(final Settings aSettings, final String pattern) throws IOException {
        if (pattern == null) {
            return;
        }

        final String path = aSettings.get("path");

        final Queue<PathFile> pathFiles = new Finder().find(
                aSettings.get("base"), aSettings.get("basepattern"),
                path == null ? "." : path, pattern)
            .sortBy(aSettings.get("sort_by", "lastmodified"))
            .order(aSettings.get("order", "asc"))
            .getPathFiles(aSettings.getAsInt("max", -1));

        for (final PathFile pathFile : pathFiles) {
            final String fileName = pathFile.toString();

            mLogger.debug("Adding file: {}", fileName);

            mQueue.add(fileName);
        }
    }

}
