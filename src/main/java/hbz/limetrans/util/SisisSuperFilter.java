package hbz.limetrans.util;

import org.metafacture.flowcontrol.StreamBuffer;
import org.metafacture.framework.StreamReceiver;
import org.metafacture.framework.helpers.DefaultStreamPipe;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class SisisSuperFilter extends DefaultStreamPipe<StreamReceiver> {

    private static final String REF_ID_ENTITY = "0004";
    private static final String REF_ID_LITERAL = "001";

    private static final String REC_ID_ENTITY = "0572";
    private static final String REC_ID_LITERAL = "011";

    private static final String SUPREC_ENTITY = "9010";
    private static final String SUPREC_LITERAL = "001";

    private static final String REC_TI_ENTITY = "0331";
    private static final String REC_TI_LITERAL = "001";

    private static final String SUP_TI_ENTITY = "0451";
    private static final String SUP_TI_LITERAL = "001";

    private static final String SUP_ID_ENTITY = "0010";
    private static final String SUP_ID_LITERAL = "001";

    private static final String SUP_ID_OUT_ENTITY = "0453";
    private static final String SUP_ID_OUT_LITERAL = "001";

    private static final String TI_ADDENDUM_ENTITY = "0335";

    private static final String SUPER_PREFIX = "(DE-600)";

    private final Map<String, StreamBuffer> mEntries = new TreeMap<>();

    private final Map<String, String> mRecIdMap = new HashMap<>();
    private final Map<String, String> mRecTiMap = new HashMap<>();
    private final Map<String, String> mRefIdMap = new HashMap<>();
    private final Map<String, String> mSupIdMap = new HashMap<>();

    private final Map<String, Boolean> mSupTiMap = new HashMap<>();

    private final Map<String, Map<String, String>> mTiAddendumMap = new HashMap<>();
    private final Map<String, Map<String, String>> mTiMainMap = new HashMap<>();

    private StreamBuffer mCurrentBuffer;
    private String mCurrentEntity;
    private String mCurrentIdentifier;

    public SisisSuperFilter() {
    }

    @Override
    public void startRecord(final String aIdentifier) {
        mCurrentIdentifier = aIdentifier;

        mCurrentBuffer = new StreamBuffer();
        mCurrentBuffer.setReceiver(getReceiver());
        mCurrentBuffer.startRecord(aIdentifier);

        mEntries.put(aIdentifier, mCurrentBuffer);
    }

    @Override
    public void endRecord() {
        // keep records "open"
    }

    @Override
    public void startEntity(final String aName) {
        mCurrentBuffer.startEntity(aName);
        mCurrentEntity = aName;
    }

    @Override
    public void endEntity() {
        mCurrentBuffer.endEntity();
        mCurrentEntity = null;
    }

    @Override // checkstyle-disable-line CyclomaticComplexity
    public void literal(final String aName, final String aValue) {
        mCurrentBuffer.literal(aName, aValue);

        if (REF_ID_ENTITY.equals(mCurrentEntity) && REF_ID_LITERAL.equals(aName)) {
            mRefIdMap.put(mCurrentIdentifier, aValue);
        }
        else if (REC_ID_ENTITY.equals(mCurrentEntity) && REC_ID_LITERAL.equals(aName)) {
            mRecIdMap.put(mCurrentIdentifier, aValue);
        }
        else if (SUP_ID_ENTITY.equals(mCurrentEntity) && SUP_ID_LITERAL.equals(aName)) {
            mSupIdMap.put(mCurrentIdentifier, aValue);
        }
        else if (REC_TI_ENTITY.equals(mCurrentEntity)) {
            if (REC_TI_LITERAL.equals(aName)) {
                mRecTiMap.put(mCurrentIdentifier, aValue);
            }

            mTiMainMap.computeIfAbsent(mCurrentIdentifier, k -> new TreeMap<>()).put(aName, aValue);
        }
        else if (SUP_TI_ENTITY.equals(mCurrentEntity)) {
            mSupTiMap.put(mCurrentIdentifier, true);
        }
        else if (TI_ADDENDUM_ENTITY.equals(mCurrentEntity)) {
            mTiAddendumMap.computeIfAbsent(mCurrentIdentifier, k -> new TreeMap<>()).put(aName, aValue);
        }
    }

    @Override
    protected void onResetStream() {
        reset();
    }

    @Override
    protected void onCloseStream() {
        mEntries.forEach((k, v) -> {
            final String refId = mRefIdMap.get(k);
            if (refId != null && !mSupIdMap.containsKey(k)) {
                v.replay();

                emit(REC_TI_ENTITY, mTiMainMap.get(refId));
                emit(TI_ADDENDUM_ENTITY, mTiAddendumMap.get(refId));

                emit(SUP_TI_ENTITY, SUP_TI_LITERAL, mSupTiMap.getOrDefault(k, false) ? null : mRecTiMap.get(refId));

                final String recId = mRecIdMap.get(refId);
                if (recId != null) {
                    emit(SUP_ID_OUT_ENTITY, SUP_ID_OUT_LITERAL, SUPER_PREFIX + recId);
                    emit(SUPREC_ENTITY, SUPREC_LITERAL, mSupIdMap.get(refId));
                }

                getReceiver().endRecord();
            }
        });

        reset();
    }

    private void emit(final String aEntity, final Map<String, String> aMap) {
        if (aMap != null) {
            final StreamReceiver receiver = getReceiver();

            receiver.startEntity(aEntity);
            aMap.forEach(receiver::literal);
            receiver.endEntity();
        }
    }

    private void emit(final String aEntity, final String aLiteral, final String aValue) {
        if (aValue != null) {
            final StreamReceiver receiver = getReceiver();

            receiver.startEntity(aEntity);
            receiver.literal(aLiteral, aValue);
            receiver.endEntity();
        }
    }

    private void reset() {
        mEntries.values().forEach(StreamBuffer::clear);
        mEntries.clear();

        mRecIdMap.clear();
        mRecTiMap.clear();
        mRefIdMap.clear();
        mSupIdMap.clear();

        mSupTiMap.clear();

        mTiAddendumMap.clear();
        mTiMainMap.clear();

        mCurrentBuffer = null;
        mCurrentEntity = null;
        mCurrentIdentifier = null;
    }

}
