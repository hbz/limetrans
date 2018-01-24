package hbz.limetrans;

import hbz.limetrans.util.LimetransException;

import org.metafacture.framework.annotations.Description;
import org.metafacture.framework.helpers.DefaultObjectReceiver;
import org.metafacture.framework.helpers.DefaultStreamReceiver;
import org.metafacture.json.JsonEncoder;
import org.elasticsearch.common.settings.Settings;

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

    public ElasticsearchClient getClient() {
        return mClient;
    }

    public void flush() {
        mClient.flush();
    }

    @Override
    public void startRecord(final String id) {
        mId = id;
        mClient.inc();
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
                return new IndexBulkReceiver();
            case "update":
                return new UpdateBulkReceiver();
            case "delete":
                return new DeleteBulkReceiver();
            default:
                throw new LimetransException("Illegal bulk action: " + aBulkAction);
        }
    }

    public class IndexBulkReceiver extends DefaultObjectReceiver<String> {

        @Override
        public void process(final String json) {
            mClient.addBulkIndex(mId, json);
        }

    }

    public class UpdateBulkReceiver extends DefaultObjectReceiver<String> {

        @Override
        public void process(final String json) {
            mClient.addBulkUpdate(mId, json);
        }

    }

    public class DeleteBulkReceiver extends DefaultObjectReceiver<String> {

        @Override
        public void process(final String json) {
            mClient.addBulkDelete(mId);
        }

    }

}
