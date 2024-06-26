package hbz.limetrans;

import hbz.limetrans.util.Helpers;
import hbz.limetrans.util.Settings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
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
    private static final long MB = 1024 * 1024;

    private static final Pattern INDEX_NAME_PATTERN = Pattern.compile("([^-]+-[^-]+-)[^-]+-?(.*)");

    private static final String[] HOST_V2_PROD = new String[]{
        "zephyros.hbz-nrw.de:9300",
        "boreas.hbz-nrw.de:9300",
        "notos.hbz-nrw.de:9300"
    };

    private static final String[] HOST_V2_DEV = new String[]{
        "hera.hbz-nrw.de:9300",
        "athene.hbz-nrw.de:9300",
        "persephone.hbz-nrw.de:9300"
    };

    private static final String[] HOST_V8_PROD = new String[]{
        "digibib-es-prod1.hbz-nrw.de:9218",
        "digibib-es-prod2.hbz-nrw.de:9218",
        "digibib-es-prod3.hbz-nrw.de:9218"
    };

    private static final String[] HOST_V8_DEV = new String[]{
        "digibib-es-test1.hbz-nrw.de:9228",
        "digibib-es-test2.hbz-nrw.de:9228",
        "digibib-es-test3.hbz-nrw.de:9228"
    };

    private enum Env {

        prod(settingsBuilder -> {
            switch (ElasticsearchClient.getClientVersion()) {
                case "8":
                    setCluster(settingsBuilder, "digibib-es-prod-8");
                    setHost(settingsBuilder, HOST_V8_PROD);
                    break;
                default:
                    setCluster(settingsBuilder, "zbn");
                    setHost(settingsBuilder, HOST_V2_PROD);
            }
        }),

        dev(settingsBuilder -> {
            switch (ElasticsearchClient.getClientVersion()) {
                case "8":
                    setCluster(settingsBuilder, "digibib-es-test-8");
                    setHost(settingsBuilder, HOST_V8_DEV);
                    break;
                default:
                    setCluster(settingsBuilder, "hap");
                    setHost(settingsBuilder, HOST_V2_DEV);
            }
        }),

        d7test(settingsBuilder -> {
            setCluster(settingsBuilder, "zbn");
            setHost(settingsBuilder, HOST_V2_DEV, 0);
            setMaxAge(settingsBuilder, -1);

            if (settingsBuilder.getSetting(new String[]{"output", "elasticsearch", "index", "name"}) instanceof final String indexName) {
                final Matcher matcher = INDEX_NAME_PATTERN.matcher(indexName);
                if (!matcher.matches()) {
                    throw new RuntimeException("Invalid index name: " + indexName);
                }

                settingsBuilder.put(new String[]{"output", "elasticsearch", "index", "name"}, matcher.group(1) + "d7test-" + matcher.group(2));
                settingsBuilder.put(new String[]{"output", "elasticsearch", "index", "timewindow"}, "");
            }
            else {
                LOGGER.warn("Missing index name setting.");
            }
        }),

        local(settingsBuilder -> {
            final String port = switch (ElasticsearchClient.getClientVersion()) {
                case "8" -> "9208";
                default  -> "9300";
            };

            setCluster(settingsBuilder, "elasticsearch");
            setHost(settingsBuilder, "localhost:" + port);
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

        private static boolean hasElasticsearchOutput(final Settings.Builder aSettingsBuilder) {
            return aSettingsBuilder.getSetting(new String[]{"output", "elasticsearch"}) != null;
        }

        private static void setCluster(final Settings.Builder aSettingsBuilder, final String aCluster) {
            if (hasElasticsearchOutput(aSettingsBuilder)) {
                aSettingsBuilder.put(new String[]{"output", "elasticsearch", "cluster"}, aCluster);
            }
        }

        private static void setHost(final Settings.Builder aSettingsBuilder, final String... aHost) {
            setHost(aSettingsBuilder, aHost, aHost.length / 2);
        }

        private static void setHost(final Settings.Builder aSettingsBuilder, final String[] aHost, final int aReplicaCount) {
            if (hasElasticsearchOutput(aSettingsBuilder)) {
                aSettingsBuilder.put(new String[]{"output", "elasticsearch", "host"}, aHost);
                aSettingsBuilder.put(new String[]{"output", "elasticsearch", "index", "number_of_replicas"}, aReplicaCount);
            }
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

        try {
            new Limetrans(setup(aArgs)).process();
        }
        finally {
            final Long rss = Helpers.getRss();
            final MemoryUsage heap = MemLog.getHeap();
            final MemoryUsage nonHeap = MemLog.getNonHeap();

            LOGGER.info("Final Memory: {}M/{}M" +
                    (rss != null ? String.format(" (%dM)", rss) : "") +
                    (memlog != null ? String.format(" [%s]", memlog) : ""),
                    (heap.getUsed() + nonHeap.getUsed()) / MB, (heap.getCommitted() + nonHeap.getCommitted()) / MB);
        }
    }

    private static Settings setup(final String[] aArgs) throws IOException {
        if (aArgs.length < 1) {
            throw new IllegalArgumentException("Could not process Limetrans: configuration missing.");
        }

        if (aArgs.length > 1) {
            throw new IllegalArgumentException("Could not process Limetrans: too many arguments: " + Arrays.toString(aArgs));
        }

        final String arg = aArgs[0].trim();
        if (arg.isEmpty()) {
            throw new IllegalArgumentException("Could not process Limetrans: empty configuration argument.");
        }

        return Helpers.getEnumProperty("env", null, Env.ignore, LOGGER::info, null).settings(arg);
    }

    private static class MemLog {

        private static final MemoryMXBean MBEAN = ManagementFactory.getMemoryMXBean();

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

                    final Long rss = Helpers.getRss();

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
                    mMaxUsed.get(), mTotalUsed.sum() / mCount.sum(),
                    mMaxCommitted.get(), mTotalCommitted.sum() / mCount.sum(),
                    mMaxRss.get(), mTotalRss.sum() / mCount.sum());
        }

    }

}
