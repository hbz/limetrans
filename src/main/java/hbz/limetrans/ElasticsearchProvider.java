package hbz.limetrans;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.IndexNotFoundException;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

public class ElasticsearchProvider {

    final private Client mClient;
    final private org.xbib.common.settings.Settings mIndexSettings;
    final private String mIndexName;
    final private String mIndexType;

    public ElasticsearchProvider(final org.xbib.common.settings.Settings aElasticsearchSettings) {
        mIndexSettings = aElasticsearchSettings.getAsSettings("index");
        mIndexName = mIndexSettings.get("name");
        mIndexType = mIndexSettings.get("type");

        final Builder clientSettingsBuilder = Settings.settingsBuilder();
        clientSettingsBuilder.put("index.name", mIndexName);
        clientSettingsBuilder.put("index.type", mIndexType);
        clientSettingsBuilder.put("cluster.name", aElasticsearchSettings.get("cluster"));

        // TODO: enable multiple server, according to array under "output.elasticsearch.host"
        final String[] host = aElasticsearchSettings.get("host.0").split(":");
        final String serverName = host[0];
        final int serverPort = Integer.valueOf(host[1]);

        try {
            mClient = TransportClient.builder().settings(clientSettingsBuilder).build().addTransportAddress(
                new InetSocketTransportAddress(InetAddress.getByName(serverName), serverPort));
        } catch (UnknownHostException ex) {
            throw new RuntimeException(ex);
        }
    }

    public Map<String, Object> getDocument(String aId) {
        final GetResponse get = mClient.prepareGet(mIndexName, mIndexType, aId).execute().actionGet();
        if (!get.isExists()){
            return null;
        }
        return get.getSource();
    }

    public void checkIndex() {
        if (!mClient.admin().indices().prepareExists(mIndexName).get().isExists()) {
            throw new IndexNotFoundException(mIndexName);
        }
    }

    public void initializeIndex() throws IOException {
        deleteIndex();

        mClient.admin().indices().prepareCreate(mIndexName)
            .setSettings(slurpFile(mIndexSettings.get("settings")))
            .addMapping(mIndexType, slurpFile(mIndexSettings.get("mapping")))
            .get();

        refreshIndex();
    }

    public void bulkIndex(final String aIndexFile) throws IOException {
        final BulkRequestBuilder bulkRequest = mClient.prepareBulk();

        try (final BufferedReader br = new BufferedReader(new InputStreamReader(
                new FileInputStream(aIndexFile), StandardCharsets.UTF_8))) {
            readData(bulkRequest, br);
        }

        bulkRequest.get();

        refreshIndex();
    }

    public void close() {
        mClient.close();
    }

    private void deleteIndex() {
        mClient.admin().cluster().prepareHealth()
            .setWaitForYellowStatus().get();

        if (mClient.admin().indices()
                .prepareExists(mIndexName).get().isExists()) {
            mClient.admin().indices()
                .prepareDelete(mIndexName).get();
        }
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

    private String slurpFile(final String aPath) throws IOException {
        return new String(Files.readAllBytes(Paths.get(aPath)));
    }

}
