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
import org.metafacture.io.TarReader;
import org.metafacture.json.JsonDecoder;
import org.metafacture.strings.StreamUnicodeNormalizer;
import org.metafacture.xml.XmlDecoder;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.attribute.FileTime;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class FileQueue implements Iterable<String> {

    private enum Processor { // checkstyle-disable-line ClassDataAbstractionCoupling

        ALMAXML(aOpener -> {
            final MarcXmlHandler handler = new MarcXmlHandler();
            handler.setNamespace("");

            return aOpener
                .setReceiver(new TarReader())
                .setReceiver(new XmlDecoder())
                .setReceiver(handler);
        }),

        FORMETA(aOpener -> aOpener
                .setReceiver(new FormetaRecordsReader())
                .setReceiver(new FormetaDecoder())),

        JSON(aOpener -> {
            final RecordReader reader = new RecordReader();
            reader.setSeparator('\0'); // read complete input

            return aOpener
                .setReceiver(reader)
                .setReceiver(getJsonDecoder());
        }),

        JSONL(aOpener -> aOpener
                .setReceiver(new LineReader())
                .setReceiver(getJsonDecoder())),

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

        private static JsonDecoder getJsonDecoder() {
            final JsonDecoder decoder = new JsonDecoder();

            decoder.setAllowComments(true); // Java/C++ style comments
            decoder.setArrayName(""); // no numbered array elements
            decoder.setRecordId(""); // no record IDs

            return decoder;
        }

        public Sender<StreamReceiver> process(final FileOpener aOpener) {
            return mFunction.apply(aOpener);
        }

    }

    private static final Logger LOGGER = LogManager.getLogger();

    private static final String GROUP_MARKER = "%GROUP_MARKER%";

    private static final FileSystem FILE_SYSTEM = FileSystems.getDefault();

    private final Processor mProcessor;
    private final Queue<String> mQueue = new LinkedList<>();
    private final boolean mNormalizeUnicode;

    public FileQueue(final Settings aSettings) throws IOException {
        if (aSettings != null) {
            mProcessor = Processor.valueOf(aSettings.get("processor", "MARCXML"));
            mNormalizeUnicode = aSettings.getAsBoolean("normalize-unicode", true);
            add(aSettings);
        }
        else {
            mProcessor = null;
            mNormalizeUnicode = false;
        }
    }

    public FileQueue(final String aProcessor, final boolean aNormalizeUnicode, final String... aFileNames) throws IOException {
        mProcessor = Processor.valueOf(aProcessor);
        mNormalizeUnicode = aNormalizeUnicode;

        for (final String fileName : aFileNames) {
            final File file = new File(fileName);

            final Settings.Builder settingsBuilder = Settings.settingsBuilder();
            settingsBuilder.put("pattern", file.getName());

            final String parent = file.getParent();
            if (parent != null) {
                settingsBuilder.put("path", parent);
            }

            add(settingsBuilder.build());
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

    public void process(final StreamReceiver aReceiver) {
        process(aReceiver, null);
    }

    public <T extends StreamReceiver & Sender<StreamReceiver>> void process(final StreamReceiver aReceiver, final T aSender) {
        final FileOpener opener = new FileOpener();
        opener.setDecompressConcatenated(true);

        if (mProcessor != null) {
            Sender<StreamReceiver> result = mProcessor.process(opener);

            if (mNormalizeUnicode) {
                result = result.setReceiver(new StreamUnicodeNormalizer());
            }

            if (aSender != null) {
                result = result.setReceiver(aSender);
            }

            result.setReceiver(aReceiver);
        }

        for (final String fileName : this) {
            final File file = new File(fileName);
            final String msg = String.format("%s file: %s [mtime=%s]",
                    mProcessor, fileName, FileTime.fromMillis(file.lastModified()));

            if (file.length() > 0) {
                LOGGER.info("Processing " + msg);

                try {
                    opener.process(fileName);
                }
                catch (final Exception e) { // checkstyle-disable-line IllegalCatch
                    LOGGER.error("Processing failed:", e);
                }
            }
            else {
                LOGGER.warn("Skipping empty " + msg);
            }
        }

        LOGGER.info("Finished processing {} files", mProcessor);

        opener.closeStream();
    }

    private void add(final Settings aSettings) throws IOException {
        LOGGER.debug("Settings: {}", aSettings);

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

                final Path file = find(aSettings, groupPattern).reduce(null, (a, b) -> b);
                if (file != null) {
                    // FIXME: Support bracket and brace expressions (`[A-Z]*.{java,class}`)
                    final Pattern p = Pattern.compile(aPattern
                            .replaceAll("[.+()]", "\\\\$0")
                            .replace("*", ".*")
                            .replace("?", ".")
                            .replace(GROUP_MARKER, "(.*)"));

                    final String name = file.getFileName().toString();
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

        find(aSettings, pattern).forEachOrdered(p -> {
            LOGGER.debug("Adding file: {}", p);
            mQueue.add(p.toString());
        });
    }

    public static Stream<Path> find(final Settings aSettings, final String aPattern) throws IOException {
        final Path path;

        if (aSettings.containsSetting("base")) {
            path = findFiles(FILE_SYSTEM.getPath(aSettings.get("base")), aSettings,
                    aSettings.get("basepattern", "*"), File::isDirectory, "name", true).iterator().next();
        }
        else {
            path = FILE_SYSTEM.getPath(aSettings.get("path", "."));
        }

        if (path == null) {
            return Stream.empty();
        }

        final int maxAge = aSettings.getAsInt("max-age", -1);
        final long threshold = System.currentTimeMillis() - maxAge * 24 * 60 * 60 * 1000;

        return findFiles(path, aSettings, aPattern, File::isFile,
                aSettings.get("sort_by", "lastmodified"), "desc".equals(aSettings.get("order")))
            .limit(aSettings.getAsInt("max", Integer.MAX_VALUE))
            .peek(p -> {
                if (maxAge > -1 && threshold > p.toFile().lastModified()) {
                    throw new RuntimeException("file too old: " + p + " (" + maxAge + ")");
                }
            });
    }

    private static Stream<Path> findFiles(final Path aPath, final Settings aSettings, final String aPattern,
            final Predicate<File> aPredicate, final String aSort, final boolean aReversed) throws IOException {
        final Comparator<Path> comparator;

        switch (aSort) {
            case "lastmodified":
                comparator = Comparator.comparing(p -> p.toFile().lastModified());
                break;
            case "name":
                comparator = Comparator.comparing(Path::toString);
                break;
            default:
                throw new RuntimeException("invalid sort parameter: " + aSort);
        }

        final PathMatcher m = FILE_SYSTEM.getPathMatcher("glob:" + aPattern);

        return Files.find(aPath, Integer.MAX_VALUE,
            (p, a) -> aPredicate.test(p.toFile()) && m.matches(p.getFileName()), FileVisitOption.FOLLOW_LINKS)
            .sorted(aReversed ? comparator.reversed() : comparator);
    }

}
