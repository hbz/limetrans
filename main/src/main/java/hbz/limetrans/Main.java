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
import java.util.stream.IntStream;

public final class Main {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final String MEMLOG = "memlog";

    private static final long MS = 1000;
    private static final long MB = 1024 * 1024;

    private static final String CLUSTER = "digibib-es-%s-%d";
    private static final String HOST = "digibib-es-%s%d.hbz-nrw.de:92%d%d";

    private static final int CLUSTER_HOSTS = 3;

    private enum Env {

        prod(settingsBuilder -> setClusterAndHost(settingsBuilder, "prod", 1)),

        dev(settingsBuilder -> setClusterAndHost(settingsBuilder, "test", 2)),

        local(settingsBuilder -> {
            final String port = "92%02d".formatted(getClientVersion());

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

        private static int getClientVersion() {
            return Integer.parseInt(ElasticsearchClient.getClientVersion());
        }

        private static boolean hasElasticsearchOutput(final Settings.Builder aSettingsBuilder) {
            return aSettingsBuilder.getSetting(new String[]{"output", "elasticsearch"}) != null;
        }

        private static void setClusterAndHost(final Settings.Builder aSettingsBuilder, final String aClusterName, final int aClusterIndex) {
            final int elasticsearchVersion = getClientVersion();
            final int clusterVersion = elasticsearchVersion % 10;

            setCluster(aSettingsBuilder, CLUSTER.formatted(aClusterName, elasticsearchVersion));
            setHost(aSettingsBuilder, IntStream.rangeClosed(1, CLUSTER_HOSTS).mapToObj(i -> HOST
                        .formatted(aClusterName, i, aClusterIndex, clusterVersion)).toArray(String[]::new));
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

        boolean failed = false;
        try {
            if (!new Limetrans(setup(aArgs)).process()) {
                failed = true;
            }
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

        System.exit(failed ? 1 : 0);
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
