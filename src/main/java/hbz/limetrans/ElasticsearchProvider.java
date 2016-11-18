package hbz.limetrans;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.xbib.common.xcontent.XContentHelper;
import org.xbib.common.xcontent.XContentService;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ElasticsearchProvider {

    final private Client mClient;
    final private Map<String, String> mCommonSettings;
    final private org.xbib.common.settings.Settings mElasticsearchSettings;
    final private boolean mIgnoreErrors;

    public ElasticsearchProvider(final org.xbib.common.settings.Settings aElasticsearchSettings) {
        mElasticsearchSettings = aElasticsearchSettings;

        mCommonSettings = new HashMap<>();
        mCommonSettings.put("index.name", aElasticsearchSettings.get("index.name"));
        mCommonSettings.put("index.type", aElasticsearchSettings.get("index.type"));
        mCommonSettings.put("cluster.name", aElasticsearchSettings.get("cluster"));

        final Builder clientSettingsBuilder = Settings.settingsBuilder().put(mCommonSettings);

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

        mIgnoreErrors = mElasticsearchSettings.getAsBoolean("index.ignoreErrors", false);
    }

    public void initializeIndex() throws IOException {
        deleteIndex();

        final CreateIndexRequestBuilder cirb =
                (new CreateIndexRequestBuilder(mClient, CreateIndexAction.INSTANCE))
                        .setIndex(mCommonSettings.get("index.name"));

        final Builder indexSettingsBuilder = Settings.builder()
            .put("number_of_shards", 1)
            .put("number_of_replicas", 1);
        mCommonSettings.forEach((key, value) -> indexSettingsBuilder.put(key, value));

        cirb.setSettings(indexSettingsBuilder.build());
        addMappings(cirb);
        try{
            cirb.execute().actionGet();
        }
        catch(Exception e) {
            if (!mIgnoreErrors) {
                throw new IOException(e);
            } else {
                System.out.println("error while creating index '" + mCommonSettings.get("index.name")
                        + "', but configured to ignore: " + e.getMessage());
            }
        }
        refreshAllIndices();
    }

    private void addMappings(CreateIndexRequestBuilder createIndexRequestBuilder) throws IOException {
        final Map<String, String> mappings = mappingsFromFile(mElasticsearchSettings.get("index.mapping"));
        Iterator it = mappings.keySet().iterator();
        while(it.hasNext()) {
            String type = (String)it.next();
            createIndexRequestBuilder.addMapping(type, mappings.get(type));
        }
    }

    public void bulkIndex(final String aIndexFile) throws IOException {
        final BulkRequestBuilder bulkRequest = mClient.prepareBulk();

        try (final BufferedReader br = new BufferedReader(new InputStreamReader(
                new FileInputStream(aIndexFile), StandardCharsets.UTF_8))) {
            readData(bulkRequest, br);
        }

        bulkRequest.execute().actionGet();

        refreshAllIndices();
    }

    public void close() {
        mClient.close();
    }

    private void deleteIndex() {
        final String indexName = mCommonSettings.get("index.name");

        mClient.admin().cluster().prepareHealth()
            .setWaitForYellowStatus().execute().actionGet();

        if (mClient.admin().indices()
                .prepareExists(indexName).execute().actionGet().isExists()) {
            mClient.admin().indices()
                .delete(new DeleteIndexRequest(indexName)).actionGet();
        }
    }

    private void readData(final BulkRequestBuilder aBulkRequest,
                          final BufferedReader aBufferedReader) throws IOException {
        final String indexType = mCommonSettings.get("index.type");
        final String indexName = mCommonSettings.get("index.name");
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
                        .prepareIndex(indexName, indexType, organisationId)
                        .setSource(organisationData));
                }
            }

            currentLine++;
        }
    }

    private void refreshAllIndices() {
        mClient.admin().indices().refresh(new RefreshRequest()).actionGet();
    }

    private Map<String, String> mappingsFromFile(String aFile) throws IOException {
        Map<String,String> mapping = new HashMap<>();
        Map<String, Object> map = XContentHelper.convertFromJsonToMap(new InputStreamReader(
                new FileInputStream(aFile)));
        for (Map.Entry<String, Object> entry : ((Map<String, Object>) map.get("properties")).entrySet()) {
            mapping.put(entry.getKey(), XContentService.jsonBuilder().map((Map) entry.getValue()).string());
        }
        return mapping;
    }

}
