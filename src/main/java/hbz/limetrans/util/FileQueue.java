package hbz.limetrans.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.culturegraph.mf.biblio.marc21.Marc21Decoder;
import org.culturegraph.mf.biblio.marc21.MarcXmlHandler;
import org.culturegraph.mf.formeta.FormetaDecoder;
import org.culturegraph.mf.formeta.FormetaRecordsReader;
import org.culturegraph.mf.framework.Sender;
import org.culturegraph.mf.framework.StreamReceiver;
import org.culturegraph.mf.io.FileOpener;
import org.culturegraph.mf.io.LineReader;
import org.culturegraph.mf.strings.StreamUnicodeNormalizer;
import org.culturegraph.mf.xml.XmlDecoder;
import org.xbib.common.settings.Settings;
import org.xbib.util.Finder;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class FileQueue implements Iterable<String> {

    private enum Processor {

        FORMETA {
            @Override
            public Sender<StreamReceiver> process(final FileOpener aOpener) {
                return aOpener
                    .setReceiver(new FormetaRecordsReader())
                    .setReceiver(new FormetaDecoder());
            }
        },

        MARC21 {
            @Override
            public Sender<StreamReceiver> process(final FileOpener aOpener) {
                return aOpener
                    .setReceiver(new LineReader())
                    .setReceiver(new Marc21Decoder());
            }
        },

        MARCXML {
            @Override
            public Sender<StreamReceiver> process(final FileOpener aOpener) {
                return aOpener
                    .setReceiver(new XmlDecoder())
                    .setReceiver(new MarcXmlHandler());
            }
        };

        public abstract Sender<StreamReceiver> process(FileOpener aOpener);

    }

    private static final Logger mLogger = LogManager.getLogger();

    private final Queue<String> mQueue = new LinkedList<>();
    private final Processor mProcessor;

    public FileQueue(final Settings aSettings) throws IOException {
        if (aSettings != null) {
            mProcessor = Processor.valueOf(aSettings.get("processor", "MARCXML"));
            add(aSettings);
        }
        else {
            mProcessor = null;
        }
    }

    public FileQueue(final String aProcessor, final String... aFileNames) throws IOException {
        mProcessor = Processor.valueOf(aProcessor);

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

    public boolean isEmpty() {
        return mQueue.isEmpty();
    }

    public int size() {
        return mQueue.size();
    }

    public void process(final StreamReceiver aReceiver, final boolean aNormalizeUnicode) {
        process(aReceiver, aNormalizeUnicode ? new StreamUnicodeNormalizer() : null);
    }

    public <T extends StreamReceiver & Sender<StreamReceiver>> void process(final StreamReceiver aReceiver, final T... aSenders) {
        final FileOpener opener = new FileOpener();

        if (mProcessor != null) {
            Sender<StreamReceiver> result = mProcessor.process(opener);

            for (final T sender : aSenders) {
                if (sender != null) {
                    result = result.setReceiver(sender);
                }
            }

            result.setReceiver(aReceiver);
        }

        for (final String fileName : this) {
            mLogger.info("Processing {} file: {}", mProcessor, fileName);
            opener.process(fileName);
        }

        mLogger.info("Finished processing {} files", mProcessor);

        opener.closeStream();
    }

    private void add(final Settings aSettings) throws IOException {
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

        new Finder().find(
                aSettings.get("base"), aSettings.get("basepattern"),
                path == null ? "." : path, pattern)
            .sortBy(aSettings.get("sort_by", "lastmodified"))
            .order(aSettings.get("order", "asc"))
            .getPathFiles(aSettings.getAsInt("max", -1))
            .stream()
            .forEachOrdered(i -> {
                mLogger.debug("Adding file: {}", i);
                mQueue.add(i.toString());
            });
    }

}
