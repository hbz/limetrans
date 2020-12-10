package hbz.limetrans;

import hbz.limetrans.util.LimetransException;
import hbz.limetrans.util.Settings;

import org.metafacture.framework.annotations.Description;
import org.metafacture.framework.helpers.DefaultObjectReceiver;
import org.metafacture.framework.helpers.DefaultStreamReceiver;
import org.metafacture.json.JsonEncoder;

@Description("Indexes an object into Elasticsearch")
public class ElasticsearchIndexer extends DefaultStreamReceiver {

    private final ElasticsearchClient mClient;
    private final JsonEncoder mJsonEncoder = new JsonEncoder();

    private String mDeletionLiteral;
    private String mId;
    private boolean mIsDeletion;

    public ElasticsearchIndexer(final ElasticsearchClient aClient, final String aBulkAction) {
        mClient = aClient;
        mJsonEncoder.setReceiver(newBulkReceiver(aBulkAction));
    }

    public ElasticsearchIndexer(final Settings aSettings) {
        this(new ElasticsearchClient(aSettings), aSettings.get("bulkAction"));
        mDeletionLiteral = aSettings.get("deletionLiteral");
    }

    public void setDeletionLiteral(final String aDeletionLiteral) {
        mDeletionLiteral = aDeletionLiteral;
    }

    public String getDeletionLiteral() {
        return mDeletionLiteral;
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
        mIsDeletion = false;

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
        if (mDeletionLiteral != null && mDeletionLiteral.equals(name)) {
            mIsDeletion = true;
        }

        mJsonEncoder.literal(name, value);
    }

    private DefaultObjectReceiver<String> newBulkReceiver(final String aBulkAction) {
        final DefaultObjectReceiver<String> receiver;

        if (aBulkAction == null) {
            receiver = new DefaultBulkReceiver();
        }
        else {
            switch (aBulkAction) {
                case "index":
                    receiver = new IndexBulkReceiver();
                    break;
                case "update":
                    receiver = new UpdateBulkReceiver();
                    break;
                case "delete":
                    receiver = new DeleteBulkReceiver();
                    break;
                default:
                    throw new LimetransException("Illegal bulk action: " + aBulkAction);
            }
        }

        return receiver;
    }

    public class DefaultBulkReceiver extends DefaultObjectReceiver<String> {

        public DefaultBulkReceiver() {
        }

        @Override
        public void process(final String json) {
            if (mIsDeletion) {
                mClient.addBulkDelete(mId);
            }
            else {
                mClient.addBulkIndex(mId, json);
            }
        }

    }

    public class IndexBulkReceiver extends DefaultObjectReceiver<String> {

        public IndexBulkReceiver() {
        }

        @Override
        public void process(final String json) {
            mClient.addBulkIndex(mId, json);
        }

    }

    public class UpdateBulkReceiver extends DefaultObjectReceiver<String> {

        public UpdateBulkReceiver() {
        }

        @Override
        public void process(final String json) {
            mClient.addBulkUpdate(mId, json);
        }

    }

    public class DeleteBulkReceiver extends DefaultObjectReceiver<String> {

        public DeleteBulkReceiver() {
        }

        @Override
        public void process(final String json) {
            mClient.addBulkDelete(mId);
        }

    }

}
