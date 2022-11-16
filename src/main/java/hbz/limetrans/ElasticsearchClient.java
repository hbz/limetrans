package hbz.limetrans;

import hbz.limetrans.util.Helpers;
import hbz.limetrans.util.LimetransException;
import hbz.limetrans.util.Settings;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.regex.Pattern;

public class ElasticsearchClient { // checkstyle-disable-line ClassDataAbstractionCoupling|ClassFanOutComplexity

    public static final String MAX_BULK_SIZE = "-1";
    public static final int MAX_BULK_ACTIONS = 1000;
    public static final int MAX_BULK_REQUESTS = 2;

    private static final Logger LOGGER = LogManager.getLogger();

    private static final String INDEX_KEY = "index";
    private static final String INDEX_NAME_KEY = "name";
    private static final String INDEX_TYPE_KEY = "type";

    private static final String INDEX_REFRESH_KEY = "refresh_interval";
    private static final String INDEX_REPLICA_KEY = "number_of_replicas";

    private static final String DEFAULT_REFRESH_INTERVAL = "30s";
    private static final Integer DEFAULT_REPLICA_COUNT = 1;

    private static final String BULK_REFRESH_INTERVAL = "-1";
    private static final Integer BULK_REPLICA_COUNT = 0;

    private static final String SETTINGS_SEPARATOR = ".";

    private final ByteSizeValue mBulkSize;
    private final Integer mNumberOfReplicas;
    private final LongAdder mDeletedCounter = new LongAdder();
    private final LongAdder mFailedCounter = new LongAdder();
    private final LongAdder mSucceededCounter = new LongAdder();
    private final Settings mIndexSettings;
    private final Settings mSettings;
    private final String mAliasName;
    private final String mIndexName;
    private final String mRefreshInterval;
    private final int mBulkActions;
    private final int mBulkRequests;

    private BulkProcessor mBulkProcessor;
    private Client mClient;
    private ElasticsearchServer mServer;
    private boolean mDeleteOnExit;
    private boolean mFailed;
    private boolean mIndexCreated;

    public ElasticsearchClient(final Settings aSettings) {
        LOGGER.debug("Settings: {}", aSettings);

        mSettings = aSettings;
        mIndexSettings = getIndexSettings(aSettings);

        mBulkActions = aSettings.getAsInt("maxbulkactions", MAX_BULK_ACTIONS);
        mBulkRequests = aSettings.getAsInt("maxbulkrequests", MAX_BULK_REQUESTS);
        mBulkSize = ByteSizeValue.parseBytesSizeValue(aSettings.get("maxbulksize", MAX_BULK_SIZE), "maxbulksize");

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
                LOGGER.info("Checking index: {}", getIndexName());
                checkIndex();
            }
            else if (delete || !indexExists()) {
                LOGGER.info("Setting up index: {}", getIndexName());
                setupIndex();
            }
        }
        catch (final RuntimeException e) { // checkstyle-disable-line IllegalCatch
            closeClient();
            throw e;
        }
    }

    public ElasticsearchClient(final String aIndexName, final String aIndexType) {
        this(Settings.settingsBuilder()
                .put(new String[]{INDEX_KEY, INDEX_NAME_KEY}, aIndexName)
                .put(new String[]{INDEX_KEY, INDEX_TYPE_KEY}, aIndexType)
                .build());
    }

    public static Settings getIndexSettings(final Settings aSettings) {
        return aSettings.getAsSettings(INDEX_KEY);
    }

    public void setClient(final String[] aHosts, final String aDataDir) {
        mClient = aDataDir == null && aHosts.length > 0 ?
            newClient(aHosts) : newClient(aDataDir);
    }

    public Client getClient() {
        return mClient;
    }

    public void setDeleteOnExit(final boolean aDeleteOnExit) {
        mDeleteOnExit = aDeleteOnExit;
    }

    public boolean getDeleteOnExit() {
        return mDeleteOnExit;
    }

    public String getDocument(final String aId) {
        return mClient
            .prepareGet(getIndexName(), getIndexType(), aId).get()
            .getSourceAsString();
    }

    public void indexDocument(final String aId, final String aDocument) {
        mClient.prepareIndex(getIndexName(), getIndexType(), aId)
            .setSource(aDocument)
            .get();
    }

    public void reset() {
        mFailed = false;
        mIndexCreated = false;
        mBulkProcessor = null;

        mFailedCounter.reset();
        mSucceededCounter.reset();

        setClient(mSettings.getAsArray("host"), mSettings.get("embeddedPath"));
        waitForYellowStatus();
    }

    public void flush() {
        closeBulk();
        refreshIndex();
    }

    public void close() {
        close(false);
    }

    public void close(final boolean aSwitchIndex) {
        closeBulk();
        refreshIndex();

        if (aSwitchIndex) {
            switchIndex();
        }

        closeClient();
    }

    private void closeClient() {
        mClient.close();

        if (mServer != null) {
            mServer.shutdown(getDeleteOnExit());
        }
    }

    public void addBulkIndex(final String aId, final String aDocument) {
        addBulk(new IndexRequest(getIndexName(), getIndexType(), aId).source(aDocument));
    }

    public void addBulkUpdate(final String aId, final String aDocument) {
        addBulk(new UpdateRequest(getIndexName(), getIndexType(), aId).doc(aDocument));
    }

    public void addBulkDelete(final String aId) {
        addBulk(new DeleteRequest(getIndexName(), getIndexType(), aId));
    }

    private void addBulk(final ActionRequest aRequest) {
        if (mBulkProcessor == null) {
            createBulk();
        }

        mBulkProcessor.add(aRequest);
    }

    private void createBulk() {
        LOGGER.info("Creating bulk processor [actions={}, requests={}, size={}]", mBulkActions, mBulkRequests, mBulkSize);

        final BulkProcessor.Listener listener = new BulkProcessor.Listener() { // checkstyle-disable-line AnonInnerLength

            @Override
            public void beforeBulk(final long aId, final BulkRequest aRequest) {
                LOGGER.debug("Before bulk {} [actions={}, bytes={}]",
                        aId, aRequest.numberOfActions(), aRequest.estimatedSizeInBytes());
            }

            @Override
            public void afterBulk(final long aId, final BulkRequest aRequest, final BulkResponse aResponse) {
                final LongAdder deleted = new LongAdder();
                final LongAdder failed = new LongAdder();
                final LongAdder succeeded = new LongAdder();

                aResponse.forEach(r -> {
                    if ("delete".equals(r.getOpType())) {
                        deleted.increment();
                    }
                    else if (r.isFailed()) {
                        failed.increment();
                        LOGGER.warn("Bulk {} item {} failed: {}", aId, r.getItemId(), r.getFailureMessage());
                    }
                    else {
                        succeeded.increment();
                    }
                });

                mDeletedCounter.add(deleted.sum());
                mFailedCounter.add(failed.sum());
                mSucceededCounter.add(succeeded.sum());

                LOGGER.debug("After bulk {} [succeeded={}, failed={}, deleted={}, took={}]",
                        aId, succeeded.sum(), failed.sum(), deleted.sum(), aResponse.getTook().millis());
            }

            @Override
            public void afterBulk(final long aId, final BulkRequest aRequest, final Throwable aThrowable) {
                LOGGER.error("Bulk " + aId + " failed: " + aThrowable.getMessage(), aThrowable);
                mFailed = true;
            }

        };

        mBulkProcessor = BulkProcessor.builder(mClient, listener)
            .setBulkActions(mBulkActions)
            .setBulkSize(mBulkSize)
            .setConcurrentRequests(mBulkRequests)
            .build();

        updateIndexSettings();
    }

    private void closeBulk() {
        if (mBulkProcessor == null) {
            return;
        }

        try {
            mBulkProcessor.flush();

            if (mBulkProcessor.awaitClose(2, TimeUnit.MINUTES)) {
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

        mBulkProcessor.close();
        mBulkProcessor = null;

        updateIndexSettings();
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

    public String getIndexType() {
        return mIndexSettings.get(INDEX_TYPE_KEY);
    }

    public String getIndexName() {
        return mIndexName;
    }

    private String getAliasName() {
        return mAliasName;
    }

    private String getAliasIndex(final String aIndex) {
        final Pattern pattern = Pattern.compile("^" + Pattern.quote(aIndex) + "\\d+$");
        final Set<String> indices = new HashSet<>();

        for (final ObjectCursor<String> indexName : mClient.admin().indices()
                .prepareGetAliases(aIndex).get().getAliases().keys()) {
            if (pattern.matcher(indexName.value).matches()) {
                indices.add(indexName.value);
            }
        }

        switch (indices.size()) {
            case 0:
                return null;
            case 1:
                return indices.iterator().next();
            default:
                throw new RuntimeException(aIndex + ": too many indices: " + indices);
        }
    }

    private String getTimeWindow() {
        final String timeWindow = mIndexSettings.get("timewindow");

        if (timeWindow == null || timeWindow.isEmpty()) {
            return null;
        }

        return DateTimeFormatter.ofPattern(timeWindow)
            .withZone(ZoneId.systemDefault())
            .format(LocalDate.now());
    }

    private void switchIndex() { // checkstyle-disable-line CyclomaticComplexity|ReturnCount
        final String aliasName = getAliasName();
        if (aliasName == null) {
            return;
        }

        final String newIndex = getIndexName();
        final String oldIndex = getAliasIndex(aliasName);

        final String suffix = " [index=" + newIndex + ", alias=" + aliasName + "]";

        if (mFailed) {
            LOGGER.warn("Failed, skipping index switch" + suffix);
            return;
        }

        if (getSucceeded() == 0) {
            LOGGER.warn("No docs, skipping index switch" + suffix);
            return;
        }

        LOGGER.info("Documents ingested: {} succeeded, {} failed, {} deleted" + suffix,
                getSucceeded(), getFailed(), getDeleted());

        if (newIndex.equals(oldIndex)) {
            return;
        }

        final IndicesAliasesRequestBuilder aliasesRequest = mClient.admin().indices()
            .prepareAliases();

        final Set<String> aliases = new HashSet<>();

        if (oldIndex == null || !indexExists(oldIndex)) {
            aliasesRequest.addAlias(newIndex, aliasName);
            aliases.add(aliasName);
        }
        else {
            for (final ObjectCursor<List<AliasMetaData>> aliasMetaDataList : mClient.admin().indices()
                    .prepareGetAliases().setIndices(oldIndex).get().getAliases().values()) {
                for (final AliasMetaData aliasMetaData : aliasMetaDataList.value) {
                    final String alias = aliasMetaData.alias();

                    aliasesRequest.removeAlias(oldIndex, alias);
                    aliases.add(alias);

                    if (aliasMetaData.filteringRequired()) {
                        aliasesRequest.addAlias(newIndex, alias,
                                new String(aliasMetaData.getFilter().uncompressed()));
                    }
                    else {
                        aliasesRequest.addAlias(newIndex, alias);
                    }
                }
            }
        }

        if (!aliases.isEmpty()) {
            LOGGER.info("Adding aliases to index {}: {}", newIndex, aliases);
            aliasesRequest.get();
        }
    }

    private Client newClient(final String[] aHosts) {
        LOGGER.info("Connecting to server: {}", String.join(", ", aHosts));

        final TransportClient client = TransportClient.builder().settings(elasticsearchSettings("transport", b -> {
            b.put(INDEX_KEY + SETTINGS_SEPARATOR + INDEX_TYPE_KEY, getIndexType());
            b.put("cluster.name", mSettings.get("cluster", "elasticsearch"));
        })).build();

        addClientTransport(client, aHosts);

        if (client.connectedNodes().isEmpty()) {
            throw new NoNodeAvailableException("No cluster nodes available: " + client.transportAddresses());
        }

        return client;
    }

    private Client newClient(final String aDataDir) {
        final AtomicReference<ElasticsearchServer> server = new AtomicReference<>();
        final Client client = ElasticsearchServer.getClient(aDataDir, server::set);

        mServer = server.get();
        return client;
    }

    private void checkIndex() {
        if (!indexExists()) {
            throw new IndexNotFoundException(getIndexName());
        }
    }

    private void setupIndex() {
        deleteIndex();
        createIndex();
        refreshIndex();
    }

    private void deleteIndex() {
        if (indexExists()) {
            LOGGER.info("Deleting index: {}", getIndexName());
            mClient.admin().indices().prepareDelete(getIndexName()).get();
        }
    }

    private void createIndex() {
        LOGGER.info("Creating index: {}", getIndexName());

        final CreateIndexRequestBuilder createRequest = mClient.admin().indices()
            .prepareCreate(getIndexName());

        setIndexSettings(createRequest);
        addIndexMapping(createRequest);

        createRequest.get();

        mIndexCreated = true;
    }

    private void refreshIndex() {
        mClient.admin().indices()
            .prepareRefresh(getIndexName()).get();
    }

    private boolean indexExists() {
        return indexExists(getIndexName());
    }

    private boolean indexExists(final String aIndexName) {
        return mClient.admin().indices()
            .prepareExists(aIndexName).get().isExists();
    }

    private void waitForYellowStatus() {
        final ClusterHealthResponse healthResponse = mClient.admin().cluster().prepareHealth()
            .setWaitForYellowStatus().setTimeout(TimeValue.timeValueSeconds(30)).get(); // checkstyle-disable-line MagicNumber

        if (healthResponse.isTimedOut()) {
            throw new RuntimeException("Cluster unhealthy: status = " + healthResponse.getStatus());
        }
    }

    private void addClientTransport(final TransportClient aClient, final String[] aHosts) {
        for (final String host : aHosts) {
            final String[] hostWithPort = host.split(":");

            try {
                aClient.addTransportAddress(new InetSocketTransportAddress(
                            InetAddress.getByName(hostWithPort[0]),
                            Integer.valueOf(hostWithPort[1])));
            }
            catch (final UnknownHostException e) {
                throw new LimetransException(e);
            }
        }
    }

    private void updateIndexSettings() {
        final boolean bulk = mBulkProcessor != null;
        final Map<String, Object> indexSettings = new HashMap<>();

        if (mNumberOfReplicas != null && mIndexCreated) {
            indexSettings.put(INDEX_REPLICA_KEY, bulk ? BULK_REPLICA_COUNT : mNumberOfReplicas);
        }

        if (mRefreshInterval != null) {
            indexSettings.put(INDEX_REFRESH_KEY, bulk ? BULK_REFRESH_INTERVAL : mRefreshInterval);
        }

        if (!indexSettings.isEmpty()) {
            final Map<String, Object> updateSettings = new HashMap<>();
            updateSettings.put(INDEX_KEY, indexSettings);

            LOGGER.info("Updating index settings {} bulk: {}: {}",
                    bulk ? "before" : "after", getIndexName(), updateSettings);

            mClient.admin().indices()
                .prepareUpdateSettings(getIndexName()).setSettings(updateSettings).get();
        }
    }

    private void setIndexSettings(final CreateIndexRequestBuilder aCreateRequest) {
        aCreateRequest.setSettings(elasticsearchIndexSettings("settings"));
    }

    private void addIndexMapping(final CreateIndexRequestBuilder aCreateRequest) {
        aCreateRequest.addMapping(getIndexType(), elasticsearchIndexSettings("mapping").getAsStructuredMap());
    }

    private org.elasticsearch.common.settings.Settings elasticsearchIndexSettings(final String aKey) {
        return elasticsearchSettings(aKey, b -> {
            try {
                b.put(Helpers.loadSettings(Helpers.getPath(getClass(),
                                mIndexSettings.get(aKey))).getAsFlatMap(SETTINGS_SEPARATOR));
                b.put(mIndexSettings.getAsSettings(aKey + "-inline").getAsFlatMap(SETTINGS_SEPARATOR));
            }
            catch (final IOException e) {
                throw new LimetransException("Failed to read index " + aKey + " file", e);
            }
        });
    }

    private org.elasticsearch.common.settings.Settings elasticsearchSettings(final String aKey,
            final Consumer<org.elasticsearch.common.settings.Settings.Builder> aConsumer) {
        final org.elasticsearch.common.settings.Settings.Builder settingsBuilder =
            org.elasticsearch.common.settings.Settings.settingsBuilder();
        aConsumer.accept(settingsBuilder);

        final org.elasticsearch.common.settings.Settings settings = settingsBuilder.build();
        LOGGER.debug("Elasticsearch {}: {}", aKey, settings.getAsStructuredMap());
        return settings;
    }

}
