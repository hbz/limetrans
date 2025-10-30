package hbz.limetrans.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.metafacture.framework.StreamReceiver;
import org.metafacture.framework.helpers.DefaultStreamPipe;
import org.metafacture.mangling.EntityPathTracker;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

public class RepeatedFieldsLogger extends DefaultStreamPipe<StreamReceiver> {

    private static final Logger LOGGER = LogManager.getLogger();

    private final EntityPathTracker mEntityPathTracker = new EntityPathTracker();
    private final Map<String, LongAdder> mRecordRepeatedFields = new HashMap<>();
    private final Map<String, LongAdder> mRepeatedFields = new HashMap<>();

    private String mRecordIdentifier;
    private boolean mPerRecord;

    public RepeatedFieldsLogger() {
    }

    public void setPerRecord(final boolean aPerRecord) {
        mPerRecord = aPerRecord;
    }

    @Override
    public void startRecord(final String aIdentifier) {
        getReceiver().startRecord(aIdentifier);
        mEntityPathTracker.startRecord(aIdentifier);

        mRecordIdentifier = aIdentifier != null && !aIdentifier.isEmpty() ? aIdentifier : "n/a";
        mRecordRepeatedFields.clear();
    }

    @Override
    public void endRecord() {
        getReceiver().endRecord();
        mEntityPathTracker.endRecord();

        final Iterator<Map.Entry<String, LongAdder>> it = mRecordRepeatedFields.entrySet().iterator();
        it.forEachRemaining(e -> {
            final long count = e.getValue().sum();

            if (count > 1) {
                getCounter(mRepeatedFields, e.getKey()).add(count);
            }
            else {
                it.remove();
            }
        });

        if (mPerRecord && !mRecordRepeatedFields.isEmpty()) {
            LOGGER.warn("Repeated fields: {}: {}", mRecordIdentifier, mRecordRepeatedFields);
        }
    }

    @Override
    public void startEntity(final String aName) {
        getReceiver().startEntity(aName);
        mEntityPathTracker.startEntity(aName);

        recordRepeatedField(mEntityPathTracker.getCurrentPath());
    }

    @Override
    public void endEntity() {
        getReceiver().endEntity();
        mEntityPathTracker.endEntity();
    }

    @Override
    public void literal(final String aName, final String aValue) {
        getReceiver().literal(aName, aValue);
        recordRepeatedField(mEntityPathTracker.getCurrentPathWith(aName));
    }

    @Override
    protected void onResetStream() {
        getReceiver().resetStream();
        mEntityPathTracker.resetStream();

        reset();
    }

    @Override
    protected void onCloseStream() {
        getReceiver().closeStream();
        mEntityPathTracker.closeStream();

        mRepeatedFields.entrySet().stream().sorted(Map.Entry.comparingByKey())
            .forEach(e -> LOGGER.warn("Repeated field: {} ({})", e.getKey(), e.getValue()));

        reset();
    }

    private void reset() {
        mRecordRepeatedFields.clear();
        mRepeatedFields.clear();
    }

    private void recordRepeatedField(final String aName) {
        getCounter(mRecordRepeatedFields, aName).increment();
    }

    private LongAdder getCounter(final Map<String, LongAdder> aMap, final String aKey) {
        return aMap.computeIfAbsent(aKey, k -> new LongAdder());
    }

}
