package hbz.limetrans.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.metafacture.metamorph.api.helpers.AbstractReadOnlyMap;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collectors;

public final class SisisSupplement extends AbstractReadOnlyMap<String, String> implements Closeable {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final String ENCODING = "UTF-8";
    private static final String SEPARATOR = "\u001F";
    private static final String LINE_DELIMITER = "\n";

    private static final String RECORD_START = "0000";
    private static final String RECORD_END = "9999";

    private static final String DEFAULT_ID_KEY = "0010";

    private final Map<String, String> mMap = new HashMap<>();
    private final String mIdKey;
    private final String mSourceKey;

    public SisisSupplement(final String aPath, final String aSourceKey) {
        this(aPath, aSourceKey, DEFAULT_ID_KEY);
    }

    public SisisSupplement(final String aPath, final String aSourceKey, final String aIdKey) {
        mSourceKey = aSourceKey;
        mIdKey = aIdKey;

        try {
            load(new File(aPath));
        }
        catch (final IOException e) {
            LOGGER.error("Loading supplement failed: " + aPath + ": " + e.getMessage(), e);
        }
    }

    public static MultiLineDecoder getDecoder() {
        final MultiLineDecoder decoder = new MultiLineDecoder();

        decoder.setRecordStart(RECORD_START);
        decoder.setRecordEnd(RECORD_END);

        return decoder;
    }

    private void load(final File aFile) throws IOException {
        final Scanner scanner = new Scanner(aFile, ENCODING);
        scanner.useDelimiter(LINE_DELIMITER);

        final MultiLineDecoder decoder = getDecoder();
        final List<String> entry = new LinkedList<>();

        int location = 0;
        String id = null;

        while (scanner.hasNext()) {
            final String line = scanner.next().trim();
            ++location;

            if (line.isEmpty()) {
                continue;
            }

            final MultiLineDecoder.Row row = decoder.parseRow(line);
            if (row == null) {
                LOGGER.warn("invalid line: {}: {}", location, line);
                continue;
            }

            if (row.isRecordEnd()) {
                if (!entry.isEmpty()) {
                    if (id == null) {
                        LOGGER.warn("missing ID {}: {}: {}", location, mIdKey, entry);
                    }
                    else {
                        if (mMap.containsKey(id)) {
                            LOGGER.warn("duplicate ID {}: {}={}", location, mIdKey, id);
                        }

                        mMap.put(id, entry.stream().collect(Collectors.joining(SEPARATOR)));
                    }

                    entry.clear();
                }

                id = null;
            }
            else {
                final String field = row.field();
                final String value = row.value();

                if (mIdKey.equals(field)) {
                    id = value;
                }

                if (mSourceKey.equals(field)) {
                    entry.add(value);
                }
            }
        }
    }

    public long count() {
        return mMap.size();
    }

    @Override
    public void close() {
        // nothing to do
    }

    @Override
    public String get(final Object aKey) {
        return getOrDefault(aKey, null);
    }

    @Override
    public String getOrDefault(final Object aKey, final String aDefault) {
        return mMap.getOrDefault(aKey, aDefault);
    }

    public static void main(final String[] aArgs) {
        final int argc = aArgs.length;
        if (argc < 2) {
            throw new IllegalArgumentException("Usage: " + SisisSupplement.class + " <path> <field> [<key>...]");
        }

        try (SisisSupplement sisisSupplement = new SisisSupplement(aArgs[0], aArgs[1])) {
            System.out.println(sisisSupplement.count());

            for (int i = 2; i < argc; ++i) {
                System.out.println(aArgs[i] + "=" + sisisSupplement.get(aArgs[i]));
            }
        }
    }

}
