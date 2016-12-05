package hbz.limetrans;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;

public class ElasticsearchProvider {

    private final Client mClient;
    private final EmbeddedElasticsearchServer mEmbeddedServer;
    private final org.xbib.common.settings.Settings mIndexSettings;
    private final String mIndexName;
    private final String mIndexType;

    public ElasticsearchProvider(final org.xbib.common.settings.Settings aElasticsearchSettings) {
        mIndexSettings = aElasticsearchSettings.getAsSettings("index");
        mIndexName = mIndexSettings.get("name");
        mIndexType = mIndexSettings.get("type");

        final String embedded = aElasticsearchSettings.get("embedded");
        final String[] hosts = aElasticsearchSettings.getAsArray("host");
        if (embedded == null && hosts.length > 0) {
            mEmbeddedServer = null;
            mClient = buildClient(hosts, aElasticsearchSettings.get("cluster", "elasticsearch"));
        }
        else {
            mEmbeddedServer = new EmbeddedElasticsearchServer(embedded);
            mClient = mEmbeddedServer.getClient();
        }
    }

    public Map<String, Object> getDocument(String aId) {
        final GetResponse response = mClient.prepareGet(mIndexName, mIndexType, aId).get();
        return response == null ? null : response.getSource();
    }

    public long getCount() {
        final SearchResponse response = mClient.prepareSearch(mIndexName).setSize(0).get();
        return response == null ? null : response.getHits().getTotalHits();
    }

    public void checkIndex() {
        if (!indexIsExists()) {
            throw new IndexNotFoundException(mIndexName);
        }
    }

    public void initializeIndex() throws IOException {
        deleteIndex();

        mClient.admin().indices().prepareCreate(mIndexName)
            .setSettings(Helpers.slurpFile(mIndexSettings.get("settings")))
            .addMapping(mIndexType, Helpers.slurpFile(mIndexSettings.get("mapping")))
            .get();

        refreshIndex();
    }

    public void bulkIndex(final String aIndexFile) throws IOException {
        final BulkRequestBuilder bulkRequest = mClient.prepareBulk();

        try (final BufferedReader br = new BufferedReader(new InputStreamReader(
                        new FileInputStream(aIndexFile), StandardCharsets.UTF_8))) {
            readData(bulkRequest, br);
        }

        final BulkResponse response = bulkRequest.get();

        refreshIndex();

        if (response != null && response.hasFailures()) {
            throw new RuntimeException(response.buildFailureMessage());
        }
    }

    public void close() {
        mClient.close();

        if (mEmbeddedServer != null) {
            mEmbeddedServer.shutdown();
        }
    }

    private Client buildClient(final String[] aHosts, final String aCluster) {
        final Builder clientSettingsBuilder = Settings.settingsBuilder()
            .put("index.name", mIndexName)
            .put("index.type", mIndexType)
            .put("cluster.name", aCluster);

        // TODO: enable multiple server, according to array under "output.elasticsearch.host"
        final String[] host = aHosts[0].split(":");
        final String serverName = host[0];
        final int serverPort = Integer.valueOf(host[1]);

        try {
            return TransportClient.builder().settings(clientSettingsBuilder).build().addTransportAddress(
                    new InetSocketTransportAddress(InetAddress.getByName(serverName), serverPort));
        }
        catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    private void deleteIndex() {
        mClient.admin().cluster().prepareHealth()
            .setWaitForYellowStatus().get();

        if (indexIsExists()) {
            mClient.admin().indices().prepareDelete(mIndexName).get();
        }
    }

    private boolean indexIsExists() {
        final IndicesExistsResponse response = mClient.admin().indices()
            .prepareExists(mIndexName).get();

        return response != null && response.isExists();
    }

    private void readData(final BulkRequestBuilder aBulkRequest,
            final BufferedReader aBufferedReader) throws IOException {
        final ObjectMapper mapper = new ObjectMapper();
        String line;
        int currentLine = 1;
        String organisationData = null;
        String[] idUriParts = null;
        String organisationId = null;

        // First line: index with id, second line: source
        while ((line = aBufferedReader.readLine()) != null) {
            JsonNode rootNode = mapper.readValue(line, JsonNode.class);

            if (currentLine % 2 != 0) {
                JsonNode index = rootNode.get("index");
                idUriParts = index.findValue("_id").asText().split("/");
                organisationId = idUriParts[idUriParts.length - 1].replace("#!", "");
            } else {
                organisationData = line;
                JsonNode libType = rootNode.get("type");

                if (libType == null || !libType.textValue().equals("Collection")) {
                    aBulkRequest.add(mClient
                            .prepareIndex(mIndexName, mIndexType, organisationId)
                            .setSource(organisationData));
                }
            }

            currentLine++;
        }
    }

    private void refreshIndex() {
        mClient.admin().indices()
            .prepareRefresh(mIndexName).get();
    }

    private class EmbeddedElasticsearchServer {

        private final Node mNode;
        private final File mTempDir;

        public EmbeddedElasticsearchServer(String aDataDir) {
            if (aDataDir == null) {
                try {
                    mTempDir = Files.createTempDirectory("limetrans-elasticsearch").toFile();
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
                .local(true)
                .settings(Settings.settingsBuilder()
                        .put("http.enabled", false)
                        .put("path.data", aDataDir)
                        .put("path.home", aDataDir)
                        .build())
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
