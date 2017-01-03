package hbz.limetrans;

import org.culturegraph.mf.framework.annotations.Description;
import org.culturegraph.mf.framework.helpers.DefaultObjectReceiver;
import org.culturegraph.mf.framework.helpers.DefaultStreamReceiver;
import org.culturegraph.mf.json.JsonEncoder;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;

@Description("Indexes an object into Elasticsearch")
public class ElasticsearchIndexer extends DefaultStreamReceiver {

    public static final String DEFAULT_BULK_ACTION = "index";

    private final ElasticsearchClient mClient;
    private final JsonEncoder mJsonEncoder = new JsonEncoder();

    private String mId = null;

    public ElasticsearchIndexer(final ElasticsearchClient aClient, final String aBulkAction) {
        mClient = aClient;

        mJsonEncoder.setReceiver(newBulkReceiver(
                    aBulkAction == null ? DEFAULT_BULK_ACTION : aBulkAction));
    }

    public ElasticsearchIndexer(final Settings aSettings) {
        this(new ElasticsearchClient(aSettings), aSettings.get("bulkAction"));
    }

    public ElasticsearchIndexer(final Map<String, String> aSettings) {
        this(Settings.settingsBuilder().put(aSettings).build());
    }

    public ElasticsearchClient getClient() {
        return mClient;
    }

    public void flush() {
        mClient.flush();
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
        mClient.reset();
    }

    @Override
    public void closeStream() {
        mJsonEncoder.closeStream();
        mClient.close(true);
    }

    @Override
    public void literal(final String name, final String value) {
        mJsonEncoder.literal(name, value);
    }

    private DefaultObjectReceiver<String> newBulkReceiver(final String aBulkAction) {
        switch (aBulkAction) {
            case "index":
                return new DefaultObjectReceiver<String>() {
                    @Override
                    public void process(final String json) {
                        mClient.addBulkIndex(mId, json);
                    }
                };
            case "update":
                return new DefaultObjectReceiver<String>() {
                    @Override
                    public void process(final String json) {
                        mClient.addBulkUpdate(mId, json);
                    }
                };
            case "delete":
                return new DefaultObjectReceiver<String>() {
                    @Override
                    public void process(final String json) {
                        mClient.addBulkDelete(mId);
                    }
                };
            default:
                throw new RuntimeException("Illegal bulk action: " + aBulkAction);
        }
    }

}
