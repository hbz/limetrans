package hbz.limetrans;

import hbz.limetrans.util.Helpers;

import org.apache.commons.io.FileUtils;
import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.helpers.DefaultObjectReceiver;
import org.culturegraph.mf.framework.helpers.DefaultStreamReceiver;
import org.culturegraph.mf.stream.converter.JsonEncoder;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.Map;

@Description("Indexes an object into Elasticsearch")
public class ElasticsearchIndexer extends DefaultStreamReceiver {

    private final JsonEncoder mJsonEncoder = new JsonEncoder();
    private final Settings mIndexSettings;
    private final String mCluster;
    private final String mEmbeddedPath;
    private final String mIndexName;
    private final String mIndexType;
    private final String[] mHosts;
    private final boolean mUpdate;

    private BulkRequestBuilder mBulkRequest;
    private Client mClient;
    private EmbeddedElasticsearchServer mEmbeddedServer;
    private String mId = null;

    public ElasticsearchIndexer(final Settings aSettings) {
        mIndexSettings = aSettings.getAsSettings("index");
        mIndexName = mIndexSettings.get("name");
        mIndexType = mIndexSettings.get("type");

        mCluster = aSettings.get("cluster", "elasticsearch");
        mEmbeddedPath = aSettings.get("embeddedPath");
        mHosts = aSettings.getAsArray("host");
        mUpdate = aSettings.getAsBoolean("update", false);

        mJsonEncoder.setReceiver(createBulkReceiver(
                    aSettings.get("bulkAction", "index")));

        reset();
    }

    public ElasticsearchIndexer(final Map<String, String> aSettings) {
        this(Settings.settingsBuilder()
                .put(aSettings)
                .build());
    }

    public Client getClient() {
        return mClient;
    }

    public void flush() {
        if (mBulkRequest.numberOfActions() > 0) {
            flushBulk();
        }
    }

    @Override
    public void startRecord(final String id) {
        mId = id;
        mJsonEncoder.startRecord(id);
    }

    @Override
    public void endRecord() {
        mJsonEncoder.endRecord();
    }

    @Override
    public void startEntity(final String name) {
        mJsonEncoder.startEntity(name);
    }

    @Override
    public void endEntity() {
        mJsonEncoder.endEntity();
    }

    @Override
    public void resetStream() {
        mJsonEncoder.resetStream();
        reset();
    }

    @Override
    public void closeStream() {
        mJsonEncoder.closeStream();
        close();
    }

    @Override
    public void literal(final String name, final String value) {
        mJsonEncoder.literal(name, value);
    }

    private void reset() {
        if (mEmbeddedPath == null && mHosts.length > 0) {
            mEmbeddedServer = null;
            mClient = buildClient();
        }
        else {
            mEmbeddedServer = new EmbeddedElasticsearchServer(mEmbeddedPath);
            mClient = mEmbeddedServer.getClient();
        }

        if (mUpdate) {
            checkIndex();
        }
        else {
            setupIndex();
        }

        newBulk();
    }

    private void close() {
        flush();

        mClient.close();

        if (mEmbeddedServer != null) {
            mEmbeddedServer.shutdown();
        }
    }

    private void newBulk() {
        mBulkRequest = mClient.prepareBulk();
    }

    private void addBulkIndex(final String json) {
        mBulkRequest.add(mClient
                .prepareIndex(mIndexName, mIndexType, mId)
                .setSource(json));
    }

    private void addBulkUpdate(final String json) {
        mBulkRequest.add(mClient
                .prepareUpdate(mIndexName, mIndexType, mId)
                .setDoc(json));
    }

    private void addBulkDelete() {
        mBulkRequest.add(mClient
                .prepareDelete(mIndexName, mIndexType, mId));
    }

    private void flushBulk() {
        final BulkResponse bulkResponse = mBulkRequest.get();

        newBulk();
        refreshIndex();

        if (bulkResponse.hasFailures()) {
            throw new RuntimeException(bulkResponse.buildFailureMessage());
        }
    }

    private DefaultObjectReceiver<String> createBulkReceiver(final String aBulkAction) {
        switch (aBulkAction) {
            case "index":
                return new DefaultObjectReceiver<String>() {
                    @Override
                    public void process(final String json) {
                        addBulkIndex(json);
                    }
                };
            case "update":
                return new DefaultObjectReceiver<String>() {
                    @Override
                    public void process(final String json) {
                        addBulkUpdate(json);
                    }
                };
            case "delete":
                return new DefaultObjectReceiver<String>() {
                    @Override
                    public void process(final String json) {
                        addBulkDelete();
                    }
                };
            default:
                throw new RuntimeException("Illegal bulk action: " + aBulkAction);
        }
    }

    private Client buildClient() {
        final TransportClient client = TransportClient.builder()
            .settings(Settings.settingsBuilder()
                    .put("index.name", mIndexName)
                    .put("index.type", mIndexType)
                    .put("cluster.name", mCluster))
            .build();

        for (final String host : mHosts) {
            final String[] hostWithPort = host.split(":");

            try {
                client.addTransportAddress(new InetSocketTransportAddress(
                            InetAddress.getByName(hostWithPort[0]),
                            Integer.valueOf(hostWithPort[1])));
            }
            catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }

        return client;
    }

    private void checkIndex() {
        if (!indexExists()) {
            throw new IndexNotFoundException(mIndexName);
        }
    }

    private void setupIndex() {
        deleteIndex();

        final CreateIndexRequestBuilder createRequest = mClient.admin().indices()
            .prepareCreate(mIndexName);

        final String settings = slurpFile("settings");
        if (settings != null) {
            createRequest.setSettings(settings);
        }

        final String mapping = slurpFile("mapping");
        if (mapping != null) {
            createRequest.addMapping(mIndexType, mapping);
        }

        createRequest.get();

        refreshIndex();
    }

    private void deleteIndex() {
        mClient.admin().cluster().prepareHealth()
            .setWaitForYellowStatus().get();

        if (indexExists()) {
            mClient.admin().indices().prepareDelete(mIndexName).get();
        }
    }

    private void refreshIndex() {
        mClient.admin().indices()
            .prepareRefresh(mIndexName).get();
    }

    private boolean indexExists() {
        return mClient.admin().indices()
            .prepareExists(mIndexName).get().isExists();
    }

    private String slurpFile(final String aKey) {
        final String path = mIndexSettings.get(aKey);

        if (path == null) {
            return null;
        }

        try {
            return Helpers.slurpFile(path);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to read " + aKey + " file", e);
        }
    }

    private class EmbeddedElasticsearchServer {

        private final Node mNode;
        private final File mTempDir;

        public EmbeddedElasticsearchServer(String aDataDir) {
            if (aDataDir == null) {
                try {
                    mTempDir = Files.createTempDirectory("elasticsearch").toFile();
                    aDataDir = mTempDir.getPath();
                }
                catch (IOException e) {
                    throw new RuntimeException("Failed to create temporary directory", e);
                }
            }
            else {
                mTempDir = null;
            }

            mNode = NodeBuilder.nodeBuilder()
                .settings(Settings.settingsBuilder()
                        .put("http.enabled", false)
                        .put("path.home", aDataDir)
                        .build())
                .local(true)
                .node();
        }

        public Client getClient() {
            return mNode.client();
        }

        public void shutdown() {
            mNode.close();

            if (mTempDir != null) {
                try {
                    FileUtils.deleteDirectory(mTempDir);
                }
                catch (IOException e) {
                    throw new RuntimeException("Failed to delete temporary directory", e);
                }
            }
        }

    }

}
