package hbz.limetrans;

import hbz.limetrans.util.Helpers;
import hbz.limetrans.util.LimetransException;
import hbz.limetrans.util.Settings;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
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
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.xbib.elasticsearch.plugin.bundle.BundlePlugin;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ElasticsearchClientV2 extends ElasticsearchClient { // checkstyle-disable-line ClassDataAbstractionCoupling|ClassFanOutComplexity

    private static final String SETTINGS_SEPARATOR = ".";

    private final ByteSizeValue mBulkSizeValue;

    private BulkProcessor mBulkProcessor;
    private Client mClient;
    private ElasticsearchServer mServer;

    public ElasticsearchClientV2(final Settings aSettings) {
        super(aSettings);

        mBulkSizeValue = ByteSizeValue.parseBytesSizeValue(getBulkSize(), "maxbulksize");
    }

    @Override
    public void reset() {
        mBulkProcessor = null;

        super.reset();
    }

    @Override
    protected void setClient(final String[] aHosts) {
        final TransportClient client = TransportClient.builder().settings(elasticsearchSettings("transport", b -> {
            b.put(INDEX_KEY + SETTINGS_SEPARATOR + INDEX_TYPE_KEY, getIndexType());
            b.put("cluster.name", getSettings().get("cluster", "elasticsearch"));
        })).build();

        for (final String host : aHosts) {
            final String[] hostWithPort = host.split(":");

            try {
                client.addTransportAddress(new InetSocketTransportAddress(
                            InetAddress.getByName(hostWithPort[0]),
                            Integer.valueOf(hostWithPort[1])));
            }
            catch (final UnknownHostException e) {
                throw new LimetransException(e);
            }
        }

        if (client.connectedNodes().isEmpty()) {
            throw new NoNodeAvailableException("No cluster nodes available: " + client.transportAddresses());
        }

        mClient = client;
    }

    @Override
    protected void setClient(final String aDataDir) {
        final AtomicReference<ElasticsearchServer> server = new AtomicReference<>();
        mClient = ElasticsearchServer.getClient(aDataDir, server::set);
        mServer = server.get();
    }

    @Override
    protected void closeClient() {
        mClient.close();

        if (mServer != null) {
            mServer.shutdown(getDeleteOnExit());
        }
    }

    @Override
    protected void indexNotFound(final String aIndexName) {
        throw new IndexNotFoundException(aIndexName);
    }

    @Override
    protected void deleteIndex(final String aIndexName) {
        mClient.admin().indices().prepareDelete(aIndexName).get();
    }

    @Override
    protected void createIndex(final String aIndexName) {
        mClient.admin().indices().prepareCreate(aIndexName)
            .setSettings(elasticsearchIndexSettings("settings"))
            .addMapping(getIndexType(), elasticsearchIndexSettings("mapping").getAsStructuredMap())
            .get();
    }

    private org.elasticsearch.common.settings.Settings elasticsearchIndexSettings(final String aKey) {
        return elasticsearchSettings(aKey, b -> {
            withSettingsFile(aKey, f -> b.put(Helpers.loadSettings(f).getAsFlatMap(SETTINGS_SEPARATOR)));
            withInlineSettings(aKey, s -> b.put(s.getAsFlatMap(SETTINGS_SEPARATOR)));
        });
    }

    private org.elasticsearch.common.settings.Settings elasticsearchSettings(final String aKey,
            final Consumer<org.elasticsearch.common.settings.Settings.Builder> aConsumer) {
        final org.elasticsearch.common.settings.Settings.Builder settingsBuilder =
            org.elasticsearch.common.settings.Settings.settingsBuilder();
        aConsumer.accept(settingsBuilder);

        final org.elasticsearch.common.settings.Settings settings = settingsBuilder.build();
        getLogger().debug("Elasticsearch {}: {}", aKey, settings.getAsStructuredMap());
        return settings;
    }

    @Override
    protected void refreshIndex(final String aIndexName) {
        mClient.admin().indices().prepareRefresh(aIndexName).get();
    }

    @Override
    protected boolean indexExists(final String aIndexName) {
        return mClient.admin().indices().prepareExists(aIndexName).get().isExists();
    }

    @Override
    protected void waitForYellowStatus() {
        final ClusterHealthResponse healthResponse = mClient.admin().cluster().prepareHealth()
            .setWaitForYellowStatus().setTimeout(TimeValue.timeValueSeconds(30)).get(); // checkstyle-disable-line MagicNumber

        if (healthResponse.isTimedOut()) {
            throw new RuntimeException("Cluster unhealthy: status = " + healthResponse.getStatus());
        }
    }

    @Override
    protected Runnable switchIndex(final String aOldIndex, final String aNewIndex, final String aAliasName, final Set<String> aAliases) {
        final IndicesAliasesRequestBuilder aliasesRequest = mClient.admin().indices().prepareAliases();

        if (aOldIndex == null || !indexExists(aOldIndex)) {
            aliasesRequest.addAlias(aNewIndex, aAliasName);
            aAliases.add(aAliasName);
        }
        else {
            for (final ObjectCursor<List<AliasMetaData>> aliasMetaDataList : mClient.admin().indices()
                    .prepareGetAliases().setIndices(aOldIndex).get().getAliases().values()) {
                for (final AliasMetaData aliasMetaData : aliasMetaDataList.value) {
                    final String alias = aliasMetaData.alias();

                    aliasesRequest.removeAlias(aOldIndex, alias);
                    aAliases.add(alias);

                    if (aliasMetaData.filteringRequired()) {
                        aliasesRequest.addAlias(aNewIndex, alias,
                                new String(aliasMetaData.getFilter().uncompressed()));
                    }
                    else {
                        aliasesRequest.addAlias(aNewIndex, alias);
                    }
                }
            }
        }

        return aliasesRequest::get;
    }

    @Override
    protected void getAliasIndexes(final String aAliasName, final Consumer<String> aConsumer) {
        for (final ObjectCursor<String> indexName : mClient.admin().indices()
                .prepareGetAliases(aAliasName).get().getAliases().keys()) {
            aConsumer.accept(indexName.value);
        }
    }

    @Override
    public Map<String, String> searchDocuments(final String aQuery) {
        return Arrays.stream(mClient.prepareSearch(getIndexName()).setQuery(aQuery).setSize(MAX_HITS).get()
                .getHits().getHits()).collect(Collectors.toMap(h -> h.getId(), h -> h.getSourceAsString()));
    }

    @Override
    public String getDocument(final String aId) {
        return mClient.prepareGet(getIndexName(), getIndexType(), aId).get().getSourceAsString();
    }

    @Override
    public void indexDocument(final String aId, final String aDocument) {
        mClient.prepareIndex(getIndexName(), getIndexType(), aId).setSource(aDocument).get();
    }

    @Override
    public void addBulkIndex(final String aId, final String aDocument) {
        addBulk(new IndexRequest(getIndexName(), getIndexType(), aId).source(aDocument));
    }

    @Override
    public void addBulkUpdate(final String aId, final String aDocument) {
        addBulk(new UpdateRequest(getIndexName(), getIndexType(), aId).doc(aDocument));
    }

    @Override
    public void addBulkDelete(final String aId) {
        addBulk(new DeleteRequest(getIndexName(), getIndexType(), aId));
    }

    private void addBulk(final ActionRequest aRequest) {
        createBulk();
        mBulkProcessor.add(aRequest);
    }

    @Override
    protected void createBulk(final int aBulkActions, final int aBulkRequests) {
        mBulkProcessor = BulkProcessor.builder(mClient, new ElasticsearchBulkListener(this))
            .setBulkActions(aBulkActions)
            .setBulkSize(mBulkSizeValue)
            .setConcurrentRequests(aBulkRequests)
            .build();
    }

    @Override
    protected boolean isBulkClosed() {
        return mBulkProcessor == null;
    }

    @Override
    protected boolean closeBulk() throws InterruptedException {
        try {
            mBulkProcessor.flush();
            return mBulkProcessor.awaitClose(2, TimeUnit.MINUTES);
        }
        finally {
            mBulkProcessor.close();
            mBulkProcessor = null;
        }
    }

    @Override
    protected void updateIndexSettings(final String aIndexName, final Map<String, Object> aSettings) {
        final Map<String, Object> settings = new HashMap<>();
        settings.put(INDEX_KEY, aSettings);

        mClient.admin().indices().prepareUpdateSettings(aIndexName).setSettings(settings).get();
    }

    private static class ElasticsearchBulkListener implements BulkProcessor.Listener {

        private final ElasticsearchClient mClient;

        private ElasticsearchBulkListener(final ElasticsearchClient aClient) {
            mClient = aClient;
        }

        @Override
        public void beforeBulk(final long aId, final BulkRequest aRequest) {
            mClient.beforeBulk(aId, aRequest.numberOfActions(), aRequest.estimatedSizeInBytes());
        }

        @Override
        public void afterBulk(final long aId, final BulkRequest aRequest, final BulkResponse aResponse) {
            mClient.afterBulk(aId, aResponse.getTook().millis(), (d, s, f) -> aResponse.forEach(r -> {
                if ("delete".equals(r.getOpType())) {
                    d.run();
                }
                else if (r.isFailed()) {
                    f.accept(String.valueOf(r.getItemId()), r.getFailureMessage());
                }
                else {
                    s.run();
                }
            }));
        }

        @Override
        public void afterBulk(final long aId, final BulkRequest aRequest, final Throwable aThrowable) {
            mClient.afterBulk(aId, aThrowable);
        }

    }

    private static class ElasticsearchServer {

        private static final Logger LOGGER = LogManager.getLogger();

        private static final Map<String, ElasticsearchServer> CACHE = new HashMap<>();

        private final File mTempDir;
        private final Node mNode;
        private final String mDataDir;

        private ElasticsearchServer(final String aDataDir, final File aTempDir) {
            mDataDir = aDataDir;
            mTempDir = aTempDir;

            final NodeBuilder nodeBuilder = NodeBuilder.nodeBuilder().settings(
                    org.elasticsearch.common.settings.Settings.settingsBuilder()
                .put("http.enabled", false)
                .put("path.home", mDataDir)
                .put("node.local", true)
            );

            mNode = new LocalNode(nodeBuilder); // nodeBuilder.build()
            mNode.start();
        }

        private static Client getClient(final String aDataDir, final Consumer<ElasticsearchServer> aConsumer) {
            final File tempDir;
            final String dataDir;

            if (aDataDir == null) {
                try {
                    tempDir = Files.createTempDirectory("elasticsearch").toFile();
                    dataDir = tempDir.getPath();
                }
                catch (final IOException e) {
                    throw new LimetransException("Failed to create directory", e);
                }
            }
            else {
                tempDir = null;
                dataDir = aDataDir;
            }

            final ElasticsearchServer server;
            if (CACHE.containsKey(dataDir)) {
                LOGGER.info("Accessing embedded server: {}", dataDir);
                server = CACHE.get(dataDir);
            }
            else {
                LOGGER.info("Starting embedded server: {}", dataDir);
                server = new ElasticsearchServer(dataDir, tempDir);

                CACHE.put(dataDir, server);

                if (aConsumer != null) {
                    aConsumer.accept(server);
                }
            }

            return server.getClient();
        }

        private Client getClient() {
            return mNode.client();
        }

        private void shutdown(final boolean aDeleteOnExit) {
            LOGGER.info("Shutting down embedded server: {}", mDataDir);

            CACHE.remove(mDataDir);
            mNode.close();

            if (mTempDir != null) {
                deleteDirectory(mTempDir);
            }
            else if (aDeleteOnExit) {
                deleteDirectory(new File(mDataDir));
            }
        }

        private void deleteDirectory(final File aDirectory) {
            try {
                FileUtils.deleteDirectory(aDirectory);
            }
            catch (final IOException e) {
                throw new LimetransException("Failed to delete directory", e);
            }
        }

        private static class LocalNode extends Node {

            private LocalNode(final NodeBuilder aBuilder) {
                super(InternalSettingsPreparer.prepareEnvironment(aBuilder.settings().build(), null),
                        Version.CURRENT, Collections.singleton(BundlePlugin.class));
            }

        }

    }

}
