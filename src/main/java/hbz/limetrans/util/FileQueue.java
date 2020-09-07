package hbz.limetrans.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.metafacture.biblio.marc21.Marc21Decoder;
import org.metafacture.biblio.marc21.MarcXmlHandler;
import org.metafacture.formeta.FormetaDecoder;
import org.metafacture.formeta.FormetaRecordsReader;
import org.metafacture.framework.Sender;
import org.metafacture.framework.StreamReceiver;
import org.metafacture.io.FileOpener;
import org.metafacture.io.LineReader;
import org.metafacture.io.RecordReader;
import org.metafacture.json.JsonDecoder;
import org.metafacture.strings.StreamUnicodeNormalizer;
import org.metafacture.xml.XmlDecoder;
import org.xbib.common.settings.Settings;
import org.xbib.util.Finder;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class FileQueue implements Iterable<String> {

    private enum Processor { // checkstyle-disable-line ClassDataAbstractionCoupling

        FORMETA(aOpener -> aOpener
                .setReceiver(new FormetaRecordsReader())
                .setReceiver(new FormetaDecoder())),

        JSON(aOpener -> {
            final RecordReader reader = new RecordReader();
            final JsonDecoder decoder = new JsonDecoder();

            reader.setSeparator('\0'); // read complete input

            decoder.setArrayName(""); // no numbered array elements
            decoder.setRecordId(""); // no record IDs

            return aOpener
                .setReceiver(reader)
                .setReceiver(decoder);
        }),

        MARC21(aOpener -> aOpener
                .setReceiver(new LineReader())
                .setReceiver(new Marc21Decoder())),

        MARC21RECORDS(aOpener -> aOpener
                .setReceiver(new RecordReader())
                .setReceiver(new Marc21Decoder())),

        MARCXML(aOpener -> aOpener
                .setReceiver(new XmlDecoder())
                .setReceiver(new MarcXmlHandler()));

        private final Function<FileOpener, Sender<StreamReceiver>> mFunction;

        Processor(final Function<FileOpener, Sender<StreamReceiver>> aFunction) {
            mFunction = aFunction;
        }

        public Sender<StreamReceiver> process(final FileOpener aOpener) {
            return mFunction.apply(aOpener);
        }

    }

    private static final Logger LOGGER = LogManager.getLogger();

    private static final String GROUP_MARKER = "%GROUP_MARKER%";

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
            if (new File(fileName).length() > 0) {
                LOGGER.info("Processing {} file: {}", mProcessor, fileName);

                try {
                    opener.process(fileName);
                }
                catch (final Exception e) { // checkstyle-disable-line IllegalCatch
                    LOGGER.error("Processing failed:", e);
                }
            }
            else {
                LOGGER.warn("Skipping empty {} file: {}", mProcessor, fileName);
            }
        }

        LOGGER.info("Finished processing {} files", mProcessor);

        opener.closeStream();
    }

    private void add(final Settings aSettings) throws IOException {
        LOGGER.debug("Settings: {}", aSettings.getAsMap());

        if (aSettings.containsSetting("patterns")) {
            for (final String pattern : aSettings.getAsArray("patterns")) {
                add(aSettings, pattern);
            }
        }
        else {
            add(aSettings, aSettings.get("pattern"));
        }
    }

    private void add(final Settings aSettings, final String aPattern) throws IOException {
        final String pattern;

        if (aPattern != null) {
            final int index = aPattern.indexOf(GROUP_MARKER);
            if (index != -1) {
                final String prefix = aPattern.substring(0, index);
                final String suffix = aPattern.substring(index + GROUP_MARKER.length());

                final String groupPattern = prefix + "*" + suffix;
                LOGGER.debug("Finding groups: {}", groupPattern);

                final Finder.PathFile file = find(aSettings, groupPattern).reduce(null, (a, b) -> b);
                if (file != null) {
                    // FIXME: Support bracket and brace expressions (`[A-Z]*.{java,class}`)
                    final Pattern p = Pattern.compile(aPattern
                            .replaceAll("[.+()]", "\\\\$0")
                            .replace("*", ".*")
                            .replace("?", ".")
                            .replace(GROUP_MARKER, "(.*)"));

                    final String name = file.getPath().getFileName().toString();
                    LOGGER.debug("Extracting group: {}: {}", p, name);

                    final Matcher m = p.matcher(name);
                    if (m.matches()) {
                        pattern = prefix + m.group(1) + suffix;
                    }
                    else {
                        pattern = null;
                    }
                }
                else {
                    pattern = null;
                }
            }
            else {
                pattern = aPattern;
            }
        }
        else {
            pattern = null;
        }

        if (pattern == null) {
            return;
        }

        LOGGER.debug("Finding pattern: {}", pattern);

        find(aSettings, pattern).forEachOrdered(i -> {
            LOGGER.debug("Adding file: {}", i);
            mQueue.add(i.toString());
        });
    }

    private Stream<Finder.PathFile> find(final Settings aSettings, final String aPattern) throws IOException {
        final String path = aSettings.get("path");

        return new Finder().find(
                aSettings.get("base"), aSettings.get("basepattern"),
                path == null ? "." : path, aPattern)
            .sortBy(aSettings.get("sort_by", "lastmodified"))
            .order(aSettings.get("order", "asc"))
            .getPathFiles(aSettings.getAsInt("max", -1))
            .stream();
    }

}
