package hbz.limetrans;

import hbz.limetrans.util.Helpers;

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

public class ElasticsearchClient {

    public static final String INDEX_NAME_KEY = "index.name";
    public static final String INDEX_TYPE_KEY = "index.type";

    private final Settings mSettings;

    private BulkRequestBuilder mBulkRequest;
    private Client mClient;
    private ElasticsearchServer mServer = null;

    public ElasticsearchClient(final Settings aSettings) {
        mSettings = aSettings;

        reset();

        if (aSettings.getAsBoolean("update", false)) {
            checkIndex();
        }
        else {
            setupIndex();
        }
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
        startBulk();
    }

    public void close() {
        flush();

        mClient.close();

        if (mServer != null) {
            mServer.shutdown();
        }
    }

    public void flush() {
        flush(mBulkRequest.numberOfActions() > 0);
    }

    public void flush(final boolean aFlush) {
        if (!aFlush) {
            return;
        }

        final BulkResponse bulkResponse = mBulkRequest.get();

        startBulk();
        refreshIndex();

        if (bulkResponse.hasFailures()) {
            onBulkFailure(bulkResponse);
        }
    }

    public void addBulkIndex(final String aId, final String aDocument) {
        mBulkRequest.add(mClient
                .prepareIndex(getIndexName(), getIndexType(), aId)
                .setSource(aDocument));
    }

    public void addBulkUpdate(final String aId, final String aDocument) {
        mBulkRequest.add(mClient
                .prepareUpdate(getIndexName(), getIndexType(), aId)
                .setDoc(aDocument));
    }

    public void addBulkDelete(final String aId) {
        mBulkRequest.add(mClient
                .prepareDelete(getIndexName(), getIndexType(), aId));
    }

    public void onBulkFailure(final BulkResponse aBulkResponse) {
        throw new RuntimeException(aBulkResponse.buildFailureMessage());
    }

    private void startBulk() {
        mBulkRequest = mClient.prepareBulk();
    }

    private String getIndexName() {
        return mSettings.get(INDEX_NAME_KEY);
    }

    private String getIndexType() {
        return mSettings.get(INDEX_TYPE_KEY);
    }

    private Client newClient(final String[] aHosts) {
        final TransportClient client = TransportClient.builder()
            .settings(getClientSettings())
            .build();

        addClientTransport(client, aHosts);

        return client;
    }

    private Client newClient(final String aDataDir) {
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
        waitForYellowStatus();

        if (indexExists()) {
            mClient.admin().indices().prepareDelete(getIndexName()).get();
        }
    }

    private void createIndex() {
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
            catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void setIndexSettings(final CreateIndexRequestBuilder aCreateRequest) {
        final String settings = getIndexSettings();
        if (settings != null) {
            aCreateRequest.setSettings(settings);
        }
    }

    private void addIndexMapping(final CreateIndexRequestBuilder aCreateRequest) {
        final String mapping = getIndexMapping();
        if (mapping != null) {
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
            return Helpers.slurpFile(path);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to read `" + aKey + "' file", e);
        }
    }

}
