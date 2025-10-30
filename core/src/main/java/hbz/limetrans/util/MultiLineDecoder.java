package hbz.limetrans.util;

import org.metafacture.framework.StreamReceiver;
import org.metafacture.framework.helpers.DefaultObjectPipe;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

// TODO: JavaDoc, @Description, @In, @Out, @FluxCommand

public final class MultiLineDecoder extends DefaultObjectPipe<String, StreamReceiver> {

    public static final String DEFAULT_FIELD_SEPARATOR = ".";
    public static final String DEFAULT_VALUE_SEPARATOR = ":";

    // checkstyle-disable-begin MemberName

    private String currentEntity;
    private String fieldSeparator = DEFAULT_FIELD_SEPARATOR;
    private String recordEnd;
    private String recordStart;
    private String valueSeparator = DEFAULT_VALUE_SEPARATOR;
    private boolean includeRecordEnd;
    private boolean includeRecordStart;

    // checkstyle-disable-end

    // checkstyle-disable-begin ParameterName

    /**
     * Creates an instance of {@link MultiLineDecoder}.
     */
    public MultiLineDecoder() {
    }

    private String getPattern(final String field) {
        return "^" + Pattern.quote(field + valueSeparator);
    }

    public void setRecordStart(final String recordStart) {
        this.recordStart = recordStart;
    }

    public String getRecordStart() {
        return recordStart;
    }

    public String getRecordStartPattern() {
        return getPattern(recordStart);
    }

    public void setIncludeRecordStart(final boolean includeRecordStart) {
        this.includeRecordStart = includeRecordStart;
    }

    public boolean getIncludeRecordStart() {
        return includeRecordStart;
    }

    public void setRecordEnd(final String recordEnd) {
        this.recordEnd = recordEnd;
    }

    public String getRecordEnd() {
        return recordEnd;
    }

    public String getRecordEndPattern() {
        return getPattern(recordEnd);
    }

    public void setIncludeRecordEnd(final boolean includeRecordEnd) {
        this.includeRecordEnd = includeRecordEnd;
    }

    public boolean getIncludeRecordEnd() {
        return includeRecordEnd;
    }

    public void setFieldSeparator(final String fieldSeparator) {
        this.fieldSeparator = fieldSeparator;
    }

    public String getFieldSeparator() {
        return fieldSeparator;
    }

    public void setValueSeparator(final String valueSeparator) {
        this.valueSeparator = Objects.requireNonNull(valueSeparator);
    }

    public String getValueSeparator() {
        return valueSeparator;
    }

    public Row parseRow(final String line) {
        final String[] p = line.split(valueSeparator, 2);
        if (p.length != 2) {
            return null;
        }

        final String field;
        final String subfield;

        final int index = fieldSeparator != null ? p[0].indexOf(fieldSeparator) : -1;
        if (index < 0) {
            field = p[0];
            subfield = null;
        }
        else {
            field = p[0].substring(0, index);
            subfield = p[0].substring(index + 1);
        }

        return new Row(field, subfield, p[1],
                Objects.equals(recordStart, field),
                Objects.equals(recordEnd, field),
                Objects.equals(currentEntity, field));
    }

    public void parseRecord(final String record, final StreamReceiver receiver) {
        if (record.trim().isEmpty()) {
            return;
        }

        final AtomicBoolean endRecord = new AtomicBoolean(true);
        final AtomicBoolean startRecord = new AtomicBoolean(recordStart == null);

        record.lines().map(this::parseRow).filter(row -> row != null).forEach(row -> {
            if (startRecord.get() || row.isRecordStart()) {
                receiver.startRecord(row.value());
                startRecord.set(false);

                if (includeRecordStart) {
                    emitField(row, receiver);
                }
            }
            else if (row.isRecordEnd()) {
                if (includeRecordEnd) {
                    emitField(row, receiver);
                }

                endRecord(receiver);
                endRecord.set(false);
            }
            else {
                emitField(row, receiver);
            }
        });

        if (endRecord.get()) {
            endRecord(receiver);
        }
    }

    private void emitField(final MultiLineDecoder.Row row, final StreamReceiver receiver) {
        final String field = row.field();
        final String value = row.value();

        final String subfield = row.subfield();
        if (subfield != null) {
            if (!row.isCurrentEntity()) {
                endEntity(receiver);
            }

            if (currentEntity == null) {
                receiver.startEntity(field);
                currentEntity = field;
            }

            receiver.literal(subfield, value);
        }
        else {
            endEntity(receiver);
            receiver.literal(field, value);
        }
    }

    private void endEntity(final StreamReceiver receiver) {
        if (currentEntity != null) {
            receiver.endEntity();
            currentEntity = null;
        }
    }

    private void endRecord(final StreamReceiver receiver) {
        endEntity(receiver);
        receiver.endRecord();
    }

    @Override
    public void process(final String record) {
        assert !isClosed();
        parseRecord(record, getReceiver());
    }

    @Override
    protected void onResetStream() {
        currentEntity = null;
    }

    public record Row(String field, String subfield, String value, boolean isRecordStart, boolean isRecordEnd, boolean isCurrentEntity) {
    }

    // checkstyle-disable-end

}
