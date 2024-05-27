package hbz.limetrans;

import hbz.limetrans.util.Helpers;
import hbz.limetrans.util.LimetransException;
import hbz.limetrans.util.Settings;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.regex.Pattern;

public abstract class ElasticsearchClient { // checkstyle-disable-line AbstractClass

    protected static final int MAX_HITS = 1000;

    protected static final String INDEX_KEY = "index";
    protected static final String INDEX_NAME_KEY = "name";
    protected static final String INDEX_TYPE_KEY = "type";

    protected static final String INDEX_REFRESH_KEY = "refresh_interval";
    protected static final String INDEX_REPLICA_KEY = "number_of_replicas";

    private static final String DEFAULT_REFRESH_INTERVAL = "30s";
    private static final Integer DEFAULT_REPLICA_COUNT = 1;

    private static final String BULK_REFRESH_INTERVAL = "-1";
    private static final Integer BULK_REPLICA_COUNT = 0;

    private static final String MAX_BULK_SIZE = "-1";
    private static final int MAX_BULK_ACTIONS = 1000;
    private static final int MAX_BULK_REQUESTS = 2;

    private static final String DEFAULT_VERSION = "2";
    private static final String VERSION_PREFIX = "V";

    private static final Logger LOGGER = LogManager.getLogger();

    private final Integer mNumberOfReplicas;
    private final LongAdder mDeletedCounter = new LongAdder();
    private final LongAdder mFailedCounter = new LongAdder();
    private final LongAdder mSucceededCounter = new LongAdder();
    private final Settings mIndexSettings;
    private final Settings mSettings;
    private final String mBulkSize;
    private final String mRefreshInterval;
    private final int mBulkActions;
    private final int mBulkRequests;

    private String mAliasName;
    private String mIndexName;
    private boolean mDeleteOnExit;
    private boolean mFailed;
    private boolean mIndexCreated;

    protected ElasticsearchClient(final Settings aSettings) {
        LOGGER.debug("Settings: {}", aSettings);

        mSettings = aSettings;
        mIndexSettings = getIndexSettings(aSettings);

        mBulkActions = aSettings.getAsInt("maxbulkactions", MAX_BULK_ACTIONS);
        mBulkRequests = aSettings.getAsInt("maxbulkrequests", MAX_BULK_REQUESTS);
        mBulkSize = aSettings.get("maxbulksize", MAX_BULK_SIZE);

        mNumberOfReplicas = mIndexSettings.getAsInt(INDEX_REPLICA_KEY, DEFAULT_REPLICA_COUNT);
        mRefreshInterval = mIndexSettings.get(INDEX_REFRESH_KEY, DEFAULT_REFRESH_INTERVAL);

        reset();

        final boolean update = aSettings.getAsBoolean("update", false);
        final boolean delete = aSettings.getAsBoolean("delete", false);

        final String indexName = mIndexSettings.get(INDEX_NAME_KEY).toLowerCase();
        final String timeWindow = getTimeWindow();

        if (timeWindow != null && !update) {
            mIndexName = indexName + timeWindow;
            mAliasName = indexName;
        }
        else {
            final String concreteName = getAliasIndex(indexName);

            mIndexName = concreteName != null ? concreteName : indexName;
            mAliasName = concreteName != null ? indexName : null;
        }

        try {
            if (update) {
                LOGGER.info("Checking index: {}", mIndexName);
                checkIndex();
            }
            else if (delete || !indexExists()) {
                LOGGER.info("Setting up index: {}", mIndexName);
                setupIndex();
            }
        }
        catch (final RuntimeException e) { // checkstyle-disable-line IllegalCatch
            closeClient();
            throw e;
        }
    }

    public void reset() {
        mFailed = false;
        mIndexCreated = false;

        mSucceededCounter.reset();
        mFailedCounter.reset();
        mDeletedCounter.reset();

        final String[] hosts = mSettings.getAsArray("host");
        final String dataDir = mSettings.get("embeddedPath");

        if (dataDir == null && hosts.length > 0) {
            LOGGER.info("Connecting to server: {}", String.join(", ", hosts));
            setClient(hosts);
        }
        else {
            setClient(dataDir);
        }

        waitForYellowStatus();
    }

    protected abstract void setClient(String[] aHosts);

    protected abstract void setClient(String aDataDir);

    protected abstract void closeClient();

    public void close() {
        close(false);
    }

    public void close(final boolean aSwitchIndex) {
        flush();

        if (aSwitchIndex) {
            switchIndex();
        }

        closeClient();
    }

    public void flush() {
        if (!isBulkClosed()) {
            try {
                if (closeBulk()) {
                    LOGGER.info("All bulk requests complete");
                }
                else {
                    LOGGER.warn("Some bulk requests still pending");
                }
            }
            catch (final InterruptedException e) {
                LOGGER.error("Flushing bulk processor interrupted", e);
                mFailed = true;
            }

            updateIndexSettings(false);
        }

        refreshIndex();
    }

    protected abstract boolean isBulkClosed();

    protected abstract boolean closeBulk() throws InterruptedException;

    protected boolean createBulk() {
        if (isBulkClosed()) {
            LOGGER.info("Creating bulk processor [actions={}, requests={}, size={}]", mBulkActions, mBulkRequests, mBulkSize);
            createBulk(mBulkActions, mBulkRequests);
            updateIndexSettings(true);
        }

        return !mFailed;
    }

    protected abstract void createBulk(int aBulkActions, int aBulkRequests);

    protected void checkIndex() {
        if (!indexExists()) {
            indexNotFound(mIndexName);
        }
    }

    protected abstract void indexNotFound(String aIndexName);

    protected void setupIndex() {
        if (indexExists()) {
            LOGGER.info("Deleting index: {}", mIndexName);
            deleteIndex(mIndexName);
        }

        LOGGER.info("Creating index: {}", mIndexName);
        createIndex(mIndexName);
        setIndexCreated(true);

        refreshIndex();
    }

    protected abstract void deleteIndex(String aIndexName);

    protected abstract void createIndex(String aIndexName);

    protected void withSettingsFile(final String aKey, final IOConsumer<Settings> aConsumer) {
        final String path = getIndexSettings().get(aKey);
        if (path == null) {
            return;
        }

        try {
            aConsumer.accept(Helpers.loadSettings(
                        Helpers.getPath(getClass(), path.formatted(getVersionTag()))));
        }
        catch (final IOException e) {
            throw new LimetransException("Failed to load index " + aKey + " file", e);
        }
    }

    protected void withInlineSettings(final String aKey, final IOConsumer<Settings> aConsumer) {
        try {
            aConsumer.accept(getIndexSettings().getAsSettings(aKey + "-inline"));
        }
        catch (final IOException e) {
            throw new LimetransException("Failed to load inline index " + aKey, e);
        }
    }

    private String getVersionTag() {
        final String name = getClass().getSimpleName();
        return name.substring(name.lastIndexOf(VERSION_PREFIX)).toLowerCase();
    }

    protected void refreshIndex() {
        refreshIndex(mIndexName);
    }

    protected abstract void refreshIndex(String aIndexName);

    protected boolean indexExists() {
        return indexExists(mIndexName);
    }

    protected abstract boolean indexExists(String aIndexName);

    protected abstract void waitForYellowStatus();

    protected void switchIndex() { // checkstyle-disable-line ReturnCount
        if (mAliasName == null) {
            return;
        }

        final String newIndex = mIndexName;
        final String oldIndex = getAliasIndex(mAliasName);

        final BiConsumer<Level, String> log = (l, m) -> LOGGER.log(l, m +
                ": {} succeeded, {} failed, {} deleted [index={}, alias={}]",
                getSucceeded(), getFailed(), getDeleted(), newIndex, mAliasName);

        if (mFailed) {
            log.accept(Level.WARN, "Failed, skipping index switch");
            return;
        }

        if (getSucceeded() == 0) {
            log.accept(Level.WARN, "No docs, skipping index switch");
            return;
        }

        log.accept(Level.INFO, "Documents ingested");

        if (newIndex.equals(oldIndex)) {
            return;
        }

        final Set<String> aliases = new HashSet<>();
        final Runnable runnable = switchIndex(oldIndex, newIndex, mAliasName, aliases);

        if (!aliases.isEmpty()) {
            LOGGER.info("Adding aliases to index {}: {}", newIndex, aliases);
            runnable.run();
        }
    }

    protected abstract Runnable switchIndex(String aOldIndex, String aNewIndex, String aAliasName, Set<String> aAliases);

    private String getAliasIndex(final String aAliasName) {
        final Pattern pattern = Pattern.compile("^" + Pattern.quote(aAliasName) + "\\d+$");
        final Set<String> indices = new HashSet<>();

        getAliasIndexes(aAliasName, i -> {
            if (pattern.matcher(i).matches()) {
                indices.add(i);
            }
        });

        switch (indices.size()) {
            case 0:
                return null;
            case 1:
                return indices.iterator().next();
            default:
                throw new RuntimeException(aAliasName + ": too many indices: " + indices);
        }
    }

    protected abstract void getAliasIndexes(String aAliasName, Consumer<String> aConsumer);

    private String getTimeWindow() {
        final String timeWindow = mIndexSettings.get("timewindow");

        if (timeWindow == null || timeWindow.isEmpty()) {
            return null;
        }

        return DateTimeFormatter.ofPattern(timeWindow)
            .withZone(ZoneId.systemDefault())
            .format(LocalDate.now());
    }

    protected void setIndexCreated(final boolean aIndexCreated) {
        mIndexCreated = aIndexCreated;
    }

    protected void setDeleteOnExit(final boolean aDeleteOnExit) {
        mDeleteOnExit = aDeleteOnExit;
    }

    protected boolean getDeleteOnExit() {
        return mDeleteOnExit;
    }

    public abstract Map<String, String> searchDocuments(String aQuery);

    public abstract String getDocument(String aId);

    public abstract void indexDocument(String aId, String aDocument);

    public abstract void addBulkIndex(String aId, String aDocument);

    public abstract void addBulkUpdate(String aId, String aDocument);

    public abstract void addBulkDelete(String aId);

    protected void beforeBulk(final long aId, final int aActions, final long aBytes) {
        LOGGER.debug("Before bulk {} [actions={}, bytes={}]", aId, aActions, aBytes);
    }

    protected void afterBulk(final long aId, final long aTook, final BulkItemConsumer aConsumer) {
        final LongAdder deleted = new LongAdder();
        final LongAdder failed = new LongAdder();
        final LongAdder succeeded = new LongAdder();

        aConsumer.accept(deleted::increment, succeeded::increment, (i, m) -> {
            mFailed = true;
            failed.increment();
            LOGGER.warn("Bulk {} item {} failed: {}", aId, i, m);
        });

        mSucceededCounter.add(succeeded.sum());
        mFailedCounter.add(failed.sum());
        mDeletedCounter.add(deleted.sum());

        LOGGER.debug("After bulk {} [succeeded={}, failed={}, deleted={}, took={}]",
                aId, succeeded.sum(), failed.sum(), deleted.sum(), aTook);
    }

    protected void afterBulk(final long aId, final Throwable aThrowable) {
        LOGGER.error("Bulk " + aId + " failed: " + aThrowable.getMessage(), aThrowable);
        mFailed = true;
    }

    protected String getBulkSize() {
        return mBulkSize;
    }

    protected Settings getSettings() {
        return mSettings;
    }

    protected Settings getIndexSettings() {
        return mIndexSettings;
    }

    public static Settings getIndexSettings(final Settings aSettings) {
        return aSettings.getAsSettings(INDEX_KEY);
    }

    public long getSucceeded() {
        return mSucceededCounter.sum();
    }

    public long getFailed() {
        return mFailedCounter.sum();
    }

    public long getDeleted() {
        return mDeletedCounter.sum();
    }

    protected String getIndexType() {
        return mIndexSettings.get(INDEX_TYPE_KEY);
    }

    protected String getIndexName() {
        return mIndexName;
    }

    public static String getClientVersion() {
        final String version = Helpers.getProperty("elasticsearchVersion");
        return version != null ? version : DEFAULT_VERSION;
    }

    public static boolean isLegacy() {
        return DEFAULT_VERSION.equals(getClientVersion());
    }

    protected static Class<? extends ElasticsearchClient> getClientClass() {
        final String version = getClientVersion();

        final Class<ElasticsearchClient> baseType = ElasticsearchClient.class;
        final Class<?> clazz;

        try {
            clazz = Class.forName(baseType.getName() + VERSION_PREFIX + version);
        }
        catch (final ReflectiveOperationException e) {
            throw new LimetransException(e);
        }

        if (!baseType.isAssignableFrom(clazz)) {
            throw new LimetransException(clazz + " must extend " + baseType);
        }

        @SuppressWarnings("unchecked") // protected by isAssignableFrom check
        final Class<? extends ElasticsearchClient> clientClass = (Class<? extends ElasticsearchClient>) clazz;

        LOGGER.info("Using Elasticsearch version " + version + ": " + clientClass);

        return clientClass;
    }

    public static ElasticsearchClient newClient(final Settings aSettings) {
        try {
            return getClientClass().getDeclaredConstructor(Settings.class).newInstance(aSettings);
        }
        catch (final ReflectiveOperationException e) {
            throw new LimetransException(e);
        }
    }

    public static ElasticsearchClient newClient(final String aIndexName, final String aIndexType, final Consumer<Settings.Builder> aConsumer) {
        final Settings.Builder settingsBuilder = Settings.settingsBuilder()
            .put(new String[]{INDEX_KEY, INDEX_NAME_KEY}, aIndexName)
            .put(new String[]{INDEX_KEY, INDEX_TYPE_KEY}, aIndexType)
            .put(new String[]{"delete"}, !isLegacy());

        if (aConsumer != null) {
            aConsumer.accept(settingsBuilder);
        }

        return newClient(settingsBuilder.build());
    }

    private void updateIndexSettings(final boolean aBulk) {
        final Map<String, Object> indexSettings = new HashMap<>();

        if (mNumberOfReplicas != null && mIndexCreated) {
            indexSettings.put(INDEX_REPLICA_KEY, aBulk ? BULK_REPLICA_COUNT : mNumberOfReplicas);
        }

        if (mRefreshInterval != null) {
            indexSettings.put(INDEX_REFRESH_KEY, aBulk ? BULK_REFRESH_INTERVAL : mRefreshInterval);
        }

        if (!indexSettings.isEmpty()) {
            LOGGER.info("Updating index settings {} bulk: {}: {}", aBulk ? "before" : "after", mIndexName, indexSettings);
            updateIndexSettings(mIndexName, indexSettings);
        }
    }

    protected abstract void updateIndexSettings(String aIndexName, Map<String, Object> aSettings);

    protected static Logger getLogger() {
        return LOGGER;
    }

    @FunctionalInterface
    protected interface IOConsumer<T> {
        void accept(T aArg) throws IOException;
    }

    @FunctionalInterface
    protected interface BulkItemConsumer {
        void accept(Runnable aDeleted, Runnable aSucceeded, BiConsumer<String, String> aFailed);
    }

}
