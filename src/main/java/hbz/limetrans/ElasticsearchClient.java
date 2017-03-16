package hbz.limetrans;

import hbz.limetrans.util.Helpers;
import hbz.limetrans.util.LimetransException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.IndexNotFoundException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class ElasticsearchClient {

    private static final Logger mLogger = LogManager.getLogger();

    public static final String INDEX_NAME_KEY = "index.name";
    public static final String INDEX_TYPE_KEY = "index.type";

    public static final int MAX_BULK_ACTIONS = 100_000;

    private final Settings mSettings;
    private final String mAliasName;
    private final String mIndexName;
    private final int mBulkActions;

    private BulkRequestBuilder mBulkRequest;
    private Client mClient;
    private ElasticsearchServer mServer = null;

    public ElasticsearchClient(final Settings aSettings) {
        mLogger.debug("Settings: {}", aSettings.getAsMap());

        mSettings = aSettings;

        final String indexName = aSettings.get(INDEX_NAME_KEY);
        final String timeWindow = getTimeWindow();

        if (timeWindow != null) {
            mIndexName = indexName + timeWindow;
            mAliasName = indexName;
        }
        else {
            mIndexName = indexName;
            mAliasName = null;
        }

        reset();

        try {
            if (aSettings.getAsBoolean("update", false)) {
                mLogger.info("Checking index: {}", getIndexName());
                checkIndex();
            }
            else if (aSettings.getAsBoolean("delete", false) || !indexExists()) {
                mLogger.info("Setting up index: {}", getIndexName());
                setupIndex();
            }
        }
        catch (final RuntimeException e) {
            close();
            throw e;
        }

        mBulkActions = aSettings.getAsInt("maxbulkactions", MAX_BULK_ACTIONS);
    }

    public ElasticsearchClient(final String aIndexName, final String aIndexType) {
        this(Settings.settingsBuilder()
                .put(INDEX_NAME_KEY, aIndexName)
                .put(INDEX_TYPE_KEY, aIndexType)
                .build());
    }

    public void setClient(final String[] aHosts, final String aDataDir) {
        mClient = aDataDir == null && aHosts.length > 0 ?
            newClient(aHosts) : newClient(aDataDir);
    }

    public Client getClient() {
        return mClient;
    }

    public void reset() {
        setClient(mSettings.getAsArray("host"), mSettings.get("embeddedPath"));
        waitForYellowStatus();
        startBulk();
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
            mLogger.info("Shutting down embedded server");
            mServer.shutdown();
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

        mLogger.info("Flushing bulk ({})", mBulkRequest.numberOfActions());

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
        mLogger.info("Starting bulk");
        mBulkRequest = mClient.prepareBulk();
    }

    private String getIndexType() {
        return mSettings.get(INDEX_TYPE_KEY);
    }

    private String getIndexName() {
        return mIndexName;
    }

    private String getAliasName() {
        return mAliasName;
    }

    private String getAliasIndex() {
        final GetAliasesResponse response = mClient.admin().indices()
            .prepareGetAliases(getAliasName()).get();

        return response.getAliases().isEmpty() ? null : response
            .getAliases().keys().iterator().next().value;
    }

    private String getTimeWindow() {
        final String timeWindow = mSettings.get("index.timewindow");

        if (timeWindow == null || timeWindow.isEmpty()) {
            return null;
        }

        return DateTimeFormatter.ofPattern(timeWindow)
            .withZone(ZoneId.systemDefault())
            .format(LocalDate.now());
    }

    private void switchIndex() {
        final String aliasName = getAliasName();

        if (aliasName != null) {
            final String newIndex = getIndexName();
            final String oldIndex = getAliasIndex();

            if (!newIndex.equals(oldIndex)) {
                mLogger.info("Switching index alias: {}", aliasName);

                final IndicesAliasesRequestBuilder aliasesRequest = mClient.admin().indices()
                    .prepareAliases();

                if (oldIndex != null) {
                    mLogger.info("Removing alias from index: {}", oldIndex);
                    aliasesRequest.removeAlias(oldIndex, aliasName);
                }

                mLogger.info("Adding alias to index: {}", newIndex);
                aliasesRequest.addAlias(newIndex, aliasName);

                aliasesRequest.get();
            }
        }
    }

    private Client newClient(final String[] aHosts) {
        mLogger.info("Connecting to server: {}", String.join(", ", aHosts));

        final TransportClient client = TransportClient.builder()
            .settings(getClientSettings())
            .build();

        addClientTransport(client, aHosts);

        return client;
    }

    private Client newClient(final String aDataDir) {
        mLogger.info("Starting embedded server: {}", aDataDir);

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
            mLogger.info("Deleting index: {}", getIndexName());
            mClient.admin().indices().prepareDelete(getIndexName()).get();
        }
    }

    private void createIndex() {
        mLogger.info("Creating index: {}", getIndexName());

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
        mClient.admin().cluster().prepareHealth()
            .setWaitForYellowStatus().get();
    }

    private Settings getClientSettings() {
        return Settings.settingsBuilder()
            .put(INDEX_NAME_KEY, getIndexName())
            .put(INDEX_TYPE_KEY, getIndexType())
            .put("cluster.name", mSettings.get("cluster", "elasticsearch"))
            .build();
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
        final String settings = getIndexSettings();
        if (settings != null) {
            mLogger.debug("Applying settings: {}", settings);
            aCreateRequest.setSettings(settings);
        }
    }

    private void addIndexMapping(final CreateIndexRequestBuilder aCreateRequest) {
        final String mapping = getIndexMapping();
        if (mapping != null) {
            mLogger.debug("Applying mapping: {}", mapping);
            aCreateRequest.addMapping(getIndexType(), mapping);
        }
    }

    private String getIndexSettings() {
        return slurpFile("index.settings");
    }

    private String getIndexMapping() {
        return slurpFile("index.mapping");
    }

    private String slurpFile(final String aKey) {
        final String path = mSettings.get(aKey);

        if (path == null) {
            return null;
        }

        try {
            return Helpers.slurpFile(path, getClass());
        }
        catch (final IOException e) {
            throw new LimetransException("Failed to read `" + aKey + "' file", e);
        }
    }

}
