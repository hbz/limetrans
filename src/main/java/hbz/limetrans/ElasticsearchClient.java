package hbz.limetrans;

import hbz.limetrans.util.Helpers;
import hbz.limetrans.util.LimetransException;
import hbz.limetrans.util.Settings;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Pattern;

public class ElasticsearchClient {

    public static final int MAX_BULK_ACTIONS = 100_000;

    private static final Logger LOGGER = LogManager.getLogger();

    private static final String INDEX_KEY = "index";
    private static final String INDEX_NAME_KEY = "name";
    private static final String INDEX_TYPE_KEY = "type";
    private static final String SETTINGS_SEPARATOR = ".";

    private final Settings mIndexSettings;
    private final Settings mSettings;
    private final String mAliasName;
    private final String mIndexName;
    private final int mBulkActions;

    private BulkRequestBuilder mBulkRequest;
    private Client mClient;
    private ElasticsearchServer mServer;
    private boolean mDeleteOnExit;
    private int numRecords;

    public ElasticsearchClient(final Settings aSettings) {
        LOGGER.debug("Settings: {}", aSettings);

        mSettings = aSettings;
        mIndexSettings = getIndexSettings(aSettings);

        reset();

        final String indexName = mIndexSettings.get(INDEX_NAME_KEY).toLowerCase();
        final String timeWindow = getTimeWindow();

        if (timeWindow != null) {
            mIndexName = indexName + timeWindow;
            mAliasName = indexName;
        }
        else {
            final String concreteName = getAliasIndex(indexName);

            mIndexName = concreteName != null ? concreteName : indexName;
            mAliasName = concreteName != null ? indexName : null;
        }

        try {
            if (aSettings.getAsBoolean("update", false)) {
                LOGGER.info("Checking index: {}", getIndexName());
                checkIndex();
            }
            else if (aSettings.getAsBoolean("delete", false) || !indexExists()) {
                LOGGER.info("Setting up index: {}", getIndexName());
                setupIndex();
            }
        }
        catch (final RuntimeException e) { // checkstyle-disable-line IllegalCatch
            close();
            throw e;
        }

        mBulkActions = aSettings.getAsInt("maxbulkactions", MAX_BULK_ACTIONS);
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
        numRecords = 0;

        setClient(mSettings.getAsArray("host"), mSettings.get("embeddedPath"));
        waitForYellowStatus();
        startBulk();
    }

    public void inc() {
        ++numRecords;
    }

    public void close() {
        close(false);
    }

    public void close(final boolean aSwitchIndex) {
        flush();

        if (aSwitchIndex) {
            switchIndex();
        }

        mClient.close();

        if (mServer != null) {
            LOGGER.info("Shutting down embedded server");
            mServer.shutdown(getDeleteOnExit());
        }
    }

    public void flush() {
        flush(1);
    }

    public void flush(final int aNum) {
        flush(mBulkRequest.numberOfActions() >= aNum);
    }

    public void flush(final boolean aFlush) {
        if (!aFlush) {
            return;
        }

        LOGGER.info("Flushing bulk ({})", mBulkRequest.numberOfActions());

        final BulkResponse bulkResponse = mBulkRequest.get();

        startBulk();
        refreshIndex();

        if (bulkResponse.hasFailures()) {
            onBulkFailure(bulkResponse);
        }
    }

    public void addBulkIndex(final String aId, final String aDocument) {
        flush(mBulkActions);
        mBulkRequest.add(mClient
                .prepareIndex(getIndexName(), getIndexType(), aId)
                .setSource(aDocument));
    }

    public void addBulkUpdate(final String aId, final String aDocument) {
        flush(mBulkActions);
        mBulkRequest.add(mClient
                .prepareUpdate(getIndexName(), getIndexType(), aId)
                .setDoc(aDocument));
    }

    public void addBulkDelete(final String aId) {
        flush(mBulkActions);
        mBulkRequest.add(mClient
                .prepareDelete(getIndexName(), getIndexType(), aId));
    }

    public void onBulkFailure(final BulkResponse aBulkResponse) {
        throw new LimetransException(aBulkResponse.buildFailureMessage());
    }

    private void startBulk() {
        LOGGER.info("Starting bulk");
        mBulkRequest = mClient.prepareBulk();
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

    private void switchIndex() { // checkstyle-disable-line ReturnCount
        final String aliasName = getAliasName();
        if (aliasName == null) {
            return;
        }

        if (numRecords == 0) {
            LOGGER.warn("No docs, skipping index switch");
            return;
        }

        final String newIndex = getIndexName();
        final String oldIndex = getAliasIndex(getAliasName());

        if (newIndex.equals(oldIndex)) {
            return;
        }

        LOGGER.info("Switching index alias: {}", aliasName);

        final IndicesAliasesRequestBuilder aliasesRequest = mClient.admin().indices()
            .prepareAliases();

        final Set<String> aliases = new HashSet<>();

        if (oldIndex == null) {
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
        LOGGER.info("Starting embedded server: {}", aDataDir);

        mServer = new ElasticsearchServer(aDataDir);
        return mServer.getClient();
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
    }

    private void refreshIndex() {
        mClient.admin().indices()
            .prepareRefresh(getIndexName()).get();
    }

    private boolean indexExists() {
        return mClient.admin().indices()
            .prepareExists(getIndexName()).get().isExists();
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
