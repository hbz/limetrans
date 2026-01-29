package hbz.limetrans;

import hbz.limetrans.util.Helpers;
import hbz.limetrans.util.LimetransException;
import hbz.limetrans.util.Settings;

import co.elastic.clients.elasticsearch._helpers.bulk.BulkIngester;
import co.elastic.clients.elasticsearch._helpers.bulk.BulkListener;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch._types.HealthStatus;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.cluster.HealthResponse;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.OperationType;
import co.elastic.clients.elasticsearch.indices.GetAliasRequest;
import co.elastic.clients.elasticsearch.indices.GetAliasResponse;
import co.elastic.clients.elasticsearch.indices.get.Feature;
import co.elastic.clients.elasticsearch.indices.get_alias.IndexAliases;
import co.elastic.clients.elasticsearch.indices.update_aliases.Action;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.BackoffPolicy;
import co.elastic.clients.transport.Version;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.util.BinaryData;
import co.elastic.clients.util.ContentType;
import co.elastic.clients.util.ObjectBuilder;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ElasticsearchClientV9 extends ElasticsearchClient { // checkstyle-disable-line ClassDataAbstractionCoupling|ClassFanOutComplexity

    private static final String CONTAINER_IMAGE = "docker.elastic.co/elasticsearch/elasticsearch:" +
        Helpers.getProperty("versions.elasticsearch9", Version.VERSION.toString()); // org.elasticsearch.Version.CURRENT

    private static final String CONTAINER_SECRET = "s3cret";

    private static final String CONTAINER_COMMAND = "bin/elasticsearch-plugin install analysis-icu && docker-entrypoint.sh eswrapper";

    private static final Map<String, ElasticsearchContainer> CONTAINER_CACHE = new HashMap<>();

    private static final int STATUS_NOT_FOUND = 404;

    private final long mBulkSizeValue;

    private BulkIngester<Void> mBulkIngester;
    private RestClientTransport mTransport;
    private co.elastic.clients.elasticsearch.ElasticsearchClient mClient;

    public ElasticsearchClientV9(final Settings aSettings) {
        super(aSettings);

        mBulkSizeValue = Long.parseLong(getBulkSize());
    }

    @Override
    public void reset() {
        mBulkIngester = null;

        super.reset();
    }

    @Override
    protected void setClient(final String[] aHosts) {
        setClient(aHosts, null);
    }

    @Override
    protected void setClient(final String aDataDir) {
        final ElasticsearchContainer container = CONTAINER_CACHE.computeIfAbsent(CONTAINER_IMAGE, k -> {
            getLogger().info("Starting embedded server: {}", k);

            final ElasticsearchContainer v = new ElasticsearchContainer(k)
                .withCreateContainerCmdModifier(c -> c.withCmd("bash", "-c", CONTAINER_COMMAND))
                .withPassword(CONTAINER_SECRET)
                .withStartupTimeout(Duration.ofMinutes(2));
            v.start();

            return v;
        });

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", CONTAINER_SECRET));

        setClient(new String[]{"https://" + container.getHttpHostAddress()}, b -> b.setHttpClientConfigCallback(c -> c
                    .setSSLContext(container.createSslContextFromCa()).setDefaultCredentialsProvider(credentialsProvider)));
    }

    private void setClient(final String[] aHosts, final Consumer<RestClientBuilder> aConsumer) {
        final HttpHost[] hosts = Arrays.stream(aHosts).map(HttpHost::create).toArray(HttpHost[]::new);

        final RestClientBuilder builder = RestClient.builder(hosts);
        if (aConsumer != null) {
            aConsumer.accept(builder);
        }

        mTransport = new RestClientTransport(builder.build(), new JacksonJsonpMapper());
        mClient = new co.elastic.clients.elasticsearch.ElasticsearchClient(mTransport);
    }

    @Override
    protected void closeClient() {
        accept(c -> mTransport.restClient().close());
        accept(c -> mTransport.close());
    }

    @Override
    protected void indexNotFound(final String aIndexName) {
        throw new RuntimeException("no such index: " + aIndexName);
    }

    @Override
    protected void deleteIndex(final String aIndexName) {
        accept(c -> c.indices().delete(b -> b.index(aIndexName)));
    }

    @Override
    protected void createIndex(final String aIndexName) {
        accept(c -> c.indices().create(b -> {
            b.index(aIndexName);

            elasticsearchIndexSettings("settings", r -> b.settings(f -> f.withJson(r)));
            elasticsearchIndexSettings("mapping", r -> b.mappings(f -> f.withJson(r)));

            return b;
        }));
    }

    private void elasticsearchIndexSettings(final String aKey, final Consumer<Reader> aConsumer) {
        try {
            final Settings.Builder settingsBuilder = Settings.settingsBuilder();

            withSettingsFile(aKey, settingsBuilder::load);
            withInlineSettings(aKey, settingsBuilder::load);

            final String settings = settingsBuilder.build().toJson();
            getLogger().debug("Elasticsearch {}: {}", aKey, settings);

            acceptReader(settings, aConsumer);
        }
        catch (final IOException e) {
            throw new LimetransException(e);
        }
    }

    @Override
    protected void refreshIndex(final String aIndexName) {
        accept(c -> c.indices().refresh(b -> b.index(aIndexName)));
    }

    @Override
    protected boolean indexExists(final String aIndexName) {
        return apply(c -> c.indices().exists(b -> b.index(aIndexName)).value());
    }

    @Override
    protected void waitForYellowStatus() {
        accept(c -> {
            final HealthResponse response = c.cluster().health(b -> b.waitForStatus(HealthStatus.Yellow));

            if (response.timedOut()) {
                throw new RuntimeException("Cluster unhealthy: status = " + response.status());
            }
        });
    }

    @Override
    protected Runnable switchIndex(final String aOldIndex, final String aNewIndex, final String aAliasName, final Set<String> aAliases) {
        final List<Action> actions = new ArrayList<>();

        final BiConsumer<String, Query> add = (a, q) -> {
            actions.add(Action.of(b -> b.add(f -> f.index(aNewIndex).alias(a).filter(q))));
            aAliases.add(a);
        };

        if (aOldIndex == null || !indexExists(aOldIndex)) {
            add.accept(aAliasName, null);
        }
        else {
            getAliases(b -> b.index(aOldIndex), (k, v) -> v.aliases().forEach((a, d) -> {
                actions.add(Action.of(b -> b.remove(f -> f.index(aOldIndex).alias(a))));
                add.accept(a, d.filter());
            }));
        }

        return () -> accept(c -> c.indices().updateAliases(b -> b.actions(actions)));
    }

    @Override
    protected void getIndexes(final String aIndex, final BiConsumer<String, IndexInfo> aConsumer) {
        accept(c -> c.indices().get(b -> b.index(aIndex).features(Feature.Aliases))
                .indices().forEach((i, s) -> accept(d -> aConsumer.accept(i, new IndexInfo(
                            !s.aliases().isEmpty(), d.count(b -> b.index(i)).count() == 0)))));
    }

    @Override
    protected void getAliasIndexes(final String aAliasName, final Consumer<String> aConsumer) {
        getAliases(b -> b.name(aAliasName), (k, v) -> aConsumer.accept(k));
    }

    private void getAliases(final Function<GetAliasRequest.Builder, ObjectBuilder<GetAliasRequest>> aFunction, final BiConsumer<String, IndexAliases> aConsumer) {
        accept(c -> {
            final GetAliasResponse response;

            try {
                response = c.indices().getAlias(aFunction);
            }
            catch (final ElasticsearchException e) {
                if (e.status() == STATUS_NOT_FOUND) {
                    getLogger().debug("Error getting aliases: " + e);
                    return;
                }
                else {
                    throw e;
                }
            }

            response.aliases().forEach(aConsumer);
        });
    }

    @Override
    public Map<String, String> searchDocuments(final String aQuery) {
        return apply(c -> applyReader(aQuery, r -> c.search(b -> b.index(getIndexName()).query(q -> q.withJson(r)).size(MAX_HITS), ObjectNode.class)))
            .hits().hits().stream().collect(Collectors.toMap(h -> h.id(), h -> h.source().toString()));
    }

    @Override
    public String getDocument(final String aId) {
        final GetResponse<ObjectNode> response = apply(c -> c.get(b -> b.index(getIndexName()).id(aId), ObjectNode.class));
        return response.found() ? response.source().toString() : null;
    }

    @Override
    public void indexDocument(final String aId, final String aDocument) {
        accept(c -> c.index(b -> b.index(getIndexName()).id(aId).document(document(aDocument))));
    }

    @Override
    public void addBulkIndex(final String aId, final String aDocument) {
        addBulk(b -> b.index(f -> f.index(getIndexName()).id(aId).document(document(aDocument))));
    }

    @Override
    public void addBulkUpdate(final String aId, final String aDocument) {
        addBulk(b -> b.update(f -> f.index(getIndexName()).id(aId).action(a -> a.doc(document(aDocument)))));
    }

    @Override
    public void addBulkDelete(final String aId) {
        addBulk(b -> b.delete(f -> f.index(getIndexName()).id(aId)));
    }

    private void addBulk(final Function<BulkOperation.Builder, ObjectBuilder<BulkOperation>> aFunction) {
        if (createBulk()) {
            mBulkIngester.add(aFunction);
        }
    }

    @Override
    protected void createBulk(final int aBulkActions, final int aBulkRequests) {
        mBulkIngester = BulkIngester.of(b -> b
                .client(mClient)
                .listener(new ElasticsearchBulkListener(this))
                .backoffPolicy(BackoffPolicy.exponentialBackoff())
                .maxOperations(aBulkActions)
                .maxSize(mBulkSizeValue)
                .maxConcurrentRequests(aBulkRequests)
        );
    }

    @Override
    protected boolean isBulkClosed() {
        return mBulkIngester == null;
    }

    @Override
    protected boolean closeBulk() throws InterruptedException {
        try {
            mBulkIngester.close();
            return mBulkIngester.pendingRequests() == 0;
        }
        finally {
            mBulkIngester = null;
        }
    }

    @Override
    protected void updateIndexSettings(final String aIndexName, final Map<String, Object> aSettings) {
        accept(c -> c.indices().putSettings(b -> b.index(aIndexName).settings(s -> {
            if (aSettings.get(INDEX_REPLICA_KEY) instanceof final Integer numberOfReplicas) {
                s.numberOfReplicas(String.valueOf(numberOfReplicas));
            }

            if (aSettings.get(INDEX_REFRESH_KEY) instanceof final String refreshInterval) {
                s.refreshInterval(f -> f.time(refreshInterval));
            }

            return s;
        })));
    }

    private BinaryData document(final String aDocument) {
        return BinaryData.of(aDocument.getBytes(), ContentType.APPLICATION_JSON);
    }

    private <T> T applyReader(final String aString, final IOFunction<Reader, T> aFunction) throws IOException {
        try (Reader reader = new StringReader(aString)) {
            return aFunction.apply(reader);
        }
    }

    private void acceptReader(final String aString, final Consumer<Reader> aConsumer) throws IOException {
        applyReader(aString, r -> {
            aConsumer.accept(r);
            return null;
        });
    }

    private <T> T apply(final IOFunction<co.elastic.clients.elasticsearch.ElasticsearchClient, T> aFunction) {
        try {
            return aFunction.apply(mClient);
        }
        catch (final IOException e) {
            throw new LimetransException(e);
        }
    }

    private void accept(final IOConsumer<co.elastic.clients.elasticsearch.ElasticsearchClient> aConsumer) {
        apply(c -> {
            aConsumer.accept(c);
            return null;
        });
    }

    @FunctionalInterface
    private interface IOFunction<T, R> {
        R apply(T aArg) throws IOException;
    }

    private static class ElasticsearchBulkListener implements BulkListener<Void> {

        private final ElasticsearchClient mClient;

        private ElasticsearchBulkListener(final ElasticsearchClient aClient) {
            mClient = aClient;
        }

        @Override
        public void beforeBulk(final long aId, final BulkRequest aRequest, final List<Void> aContexts) {
            mClient.beforeBulk(aId, aRequest.operations().size(), -1);
        }

        @Override
        public void afterBulk(final long aId, final BulkRequest aRequest, final List<Void> aContexts, final BulkResponse aResponse) {
            mClient.afterBulk(aId, aResponse.took(), (d, s, f) -> aResponse.items().forEach(r -> {
                if (r.operationType() == OperationType.Delete) {
                    d.run();
                }
                else {
                    final ErrorCause error = r.error();

                    if (error != null) {
                        f.accept(r.id(), error.reason());
                    }
                    else {
                        s.run();
                    }
                }
            }));
        }

        @Override
        public void afterBulk(final long aId, final BulkRequest aRequest, final List<Void> aContexts, final Throwable aThrowable) {
            mClient.afterBulk(aId, aThrowable);
        }

    }

}
