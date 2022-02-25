package hbz.limetrans;

import hbz.limetrans.util.Helpers;
import hbz.limetrans.util.Settings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Main {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final String MEMLOG = "memlog";

    private static final long MS = 1000;
    private static final long KB = 1024;
    private static final long MB = KB * KB;

    private static final String[] INDEX_SETTING = new String[]{"output", "elasticsearch", "index"};

    private static final Pattern INDEX_NAME_PATTERN = Pattern.compile("([^-]+-[^-]+-)[^-]+-?(.*)");

    private enum Env {

        prod(settingsBuilder -> {
            setCluster(settingsBuilder, "hap");
            setHost(settingsBuilder, "hera.hbz-nrw.de:9300", "athene.hbz-nrw.de:9300", "persephone.hbz-nrw.de:9300");
        }),

        dev(settingsBuilder -> {
            setCluster(settingsBuilder, "zbn");
            setHost(settingsBuilder, "zephyros.hbz-nrw.de:9300", "boreas.hbz-nrw.de:9300", "notos.hbz-nrw.de:9300");
        }),

        d7test(settingsBuilder -> {
            setCluster(settingsBuilder, "zbn");
            setHost(settingsBuilder, "zephyros.hbz-nrw.de:9300", "boreas.hbz-nrw.de:9300", "notos.hbz-nrw.de:9300");
            setMaxAge(settingsBuilder, -1);

            final VarargsOperator<String> indexSetting = k -> {
                final String[] result = Arrays.copyOf(INDEX_SETTING, INDEX_SETTING.length + k.length);
                System.arraycopy(k, 0, result, INDEX_SETTING.length, k.length);
                return result;
            };

            if (settingsBuilder.getSetting(indexSetting.apply("name")) instanceof final String indexName) {
                final Matcher matcher = INDEX_NAME_PATTERN.matcher(indexName);
                if (!matcher.matches()) {
                    throw new RuntimeException("Invalid index name: " + indexName);
                }

                settingsBuilder.put(indexSetting.apply("name"), matcher.group(1) + "d7test-" + matcher.group(2));
                settingsBuilder.put(indexSetting.apply("timewindow"), "");
                settingsBuilder.put(indexSetting.apply("settings-inline", "index", "number_of_replicas"), 0);
            }
            else {
                LOGGER.warn("Missing index name setting.");
            }
        }),

        local(settingsBuilder -> {
            setCluster(settingsBuilder, "elasticsearch");
            setHost(settingsBuilder, "localhost:9300");
            setMaxAge(settingsBuilder, -1);
        }),

        ignore(settingsBuilder -> {});

        private final Consumer<Settings.Builder> mConsumer;

        Env(final Consumer<Settings.Builder> aConsumer) {
            mConsumer = aConsumer;
        }

        public Settings settings(final String aConf) {
            try {
                return Helpers.loadSettings(aConf, settingsBuilder -> {
                    settingsBuilder.put("env", toString());
                    mConsumer.accept(settingsBuilder);
                });
            }
            catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }

        private static void setCluster(final Settings.Builder aSettingsBuilder, final String aCluster) {
            aSettingsBuilder.put(new String[]{"output", "elasticsearch", "cluster"}, aCluster);
        }

        private static void setHost(final Settings.Builder aSettingsBuilder, final String... aHost) {
            aSettingsBuilder.put(new String[]{"output", "elasticsearch", "host"}, aHost);
        }

        private static void setMaxAge(final Settings.Builder aSettingsBuilder, final int aMaxAge) {
            aSettingsBuilder.put(new String[]{"input", "queue", "max-age"}, aMaxAge);
        }

    }

    private Main() {
        throw new IllegalAccessError("Utility class");
    }

    public static void main(final String[] aArgs) throws IOException {
        final Integer interval = Helpers.getProperty(MEMLOG, Integer::parseInt, 10);
        final MemLog memlog = interval != null ? new MemLog(interval) : null;

        new Limetrans(setup(aArgs)).process();

        final Long rss = MemLog.getRss();
        final MemoryUsage heap = MemLog.getHeap();
        final MemoryUsage nonHeap = MemLog.getNonHeap();

        LOGGER.info("Final Memory: {}M/{}M" + (rss != null ? String.format(" (%dM)", rss) : "") + (memlog != null ? String.format(" [%s]", memlog) : ""),
                (heap.getUsed() + nonHeap.getUsed()) / MB, (heap.getCommitted() + nonHeap.getCommitted()) / MB);
    }

    private static Settings setup(final String[] aArgs) throws IOException {
        if (aArgs.length < 1) {
            throw new IllegalArgumentException("Could not process limetrans: configuration missing.");
        }

        if (aArgs.length > 1) {
            throw new IllegalArgumentException("Could not process limetrans: too many arguments: " + Arrays.toString(aArgs));
        }

        final String arg = aArgs[0].trim();
        if (arg.isEmpty()) {
            throw new IllegalArgumentException("Could not process limetrans: empty configuration argument.");
        }

        return Helpers.getEnumProperty("env", null, Env.ignore, LOGGER::info, null).settings(arg);
    }

    private interface VarargsOperator<T> {
        @SuppressWarnings("unchecked")
        T[] apply(T... aArgs);
    }

    private static class MemLog {

        private static final MemoryMXBean MBEAN = ManagementFactory.getMemoryMXBean();

        private static final Path STATUS = Paths.get("/proc/self/status");
        private static final Pattern RSS_PATTERN = Pattern.compile("\\AVmRSS:\\s+(\\d+)\\s+kB");

        private final LongAccumulator mMaxCommitted = new LongAccumulator(Long::max, Long.MIN_VALUE);
        private final LongAccumulator mMaxRss = new LongAccumulator(Long::max, Long.MIN_VALUE);
        private final LongAccumulator mMaxUsed = new LongAccumulator(Long::max, Long.MIN_VALUE);
        private final LongAdder mCount = new LongAdder();
        private final LongAdder mTotalCommitted = new LongAdder();
        private final LongAdder mTotalRss = new LongAdder();
        private final LongAdder mTotalUsed = new LongAdder();
        private final Thread mThread;

        private MemLog(final int aInterval) {
            mThread = new Thread(() -> {
                while (true) {
                    mCount.increment();

                    final MemoryUsage heap = getHeap();
                    final MemoryUsage nonHeap = getNonHeap();

                    final long heapUsed = heap.getUsed() / MB;
                    final long nonHeapUsed = nonHeap.getUsed() / MB;

                    final long heapCommitted = heap.getCommitted() / MB;
                    final long nonHeapCommitted = nonHeap.getCommitted() / MB;

                    final long used = heapUsed + nonHeapUsed;
                    mMaxUsed.accumulate(used);
                    mTotalUsed.add(used);

                    final long committed = heapCommitted + nonHeapCommitted;
                    mMaxCommitted.accumulate(committed);
                    mTotalCommitted.add(committed);

                    final StringBuilder sb = new StringBuilder(String.format(
                                "Current Memory: heap.used=%dM, nonheap.used=%dM, heap.committed=%dM, nonheap.committed=%dM",
                                heapUsed, nonHeapUsed, heapCommitted, nonHeapCommitted));

                    final Long rss = getRss();

                    if (rss != null) {
                        mMaxRss.accumulate(rss);
                        mTotalRss.add(rss);

                        sb.append(String.format(", rss=%dM", rss));
                    }

                    LOGGER.info(sb);

                    if (Thread.interrupted()) {
                        break;
                    }
                    else {
                        try {
                            Thread.sleep(aInterval * MS);
                        }
                        catch (final InterruptedException e) {
                            break;
                        }
                    }
                }
            }, MEMLOG);

            mThread.start();
        }

        private static MemoryUsage getHeap() {
            return MBEAN.getHeapMemoryUsage();
        }

        private static MemoryUsage getNonHeap() {
            return MBEAN.getNonHeapMemoryUsage();
        }

        private static Long getRss() {
            try {
                for (final String line : Files.readAllLines(STATUS)) {
                    final Matcher matcher = RSS_PATTERN.matcher(line);

                    if (matcher.matches()) {
                        return Long.parseLong(matcher.group(1)) / KB;
                    }
                }
            }
            catch (final IOException e) {
            }

            return null;
        }

        @Override
        public String toString() {
            if (mThread.isAlive()) {
                mThread.interrupt();

                try {
                    mThread.join(MS);
                }
                catch (final InterruptedException e) {
                }
            }

            return String.format("used.max=%dM, used.avg=%dM, committed.max=%dM, committed.avg=%dM, rss.max=%dM, rss.avg=%dM",
                    mMaxUsed.get(), mTotalUsed.sum() / mCount.sum(), mMaxCommitted.get(), mTotalCommitted.sum() / mCount.sum(), mMaxRss.get(), mTotalRss.sum() / mCount.sum());
        }

    }

}
