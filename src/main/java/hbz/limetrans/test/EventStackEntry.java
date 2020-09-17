package hbz.limetrans.test;

import org.metafacture.javaintegration.EventList.Event;
import org.metafacture.json.JsonEncoder;

public class EventStackEntry {

    public static final String SEPARATOR = "/";

    public enum Mismatch {

        TYPE,
        NAME,
        VALUE;

        public static final String FORMAT = "Event %s mismatch";

        @Override
        public String toString() {
            return String.format(FORMAT, name());
        }

    }

    private final EventStackEntry mParent;
    private final Event mEvent;

    private int mPosition;

    public EventStackEntry(final Event aEvent, final EventStackEntry aParent) {
        mEvent = aEvent;
        mParent = aParent;

        if (aParent != null) {
            aParent.incrementPosition();
        }
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();

        if (mParent != null) {
            builder
                .append(mParent)
                .append(SEPARATOR)
                .append(mEvent);

            mParent.appendPosition(builder);
        }

        return builder.toString();
    }

    public Event getEvent() {
        return mEvent;
    }

    public Mismatch getMismatch(final EventStackEntry aEntry) {
        final Event event = aEntry.getEvent();

        final Event.Type type = mEvent.getType();
        final String name = mEvent.getName();
        final String value = mEvent.getValue();

        final Mismatch result;

        if (type != event.getType()) {
            result = Mismatch.TYPE;
        }
        else if (name != null && !name.equals(event.getName())) {
            if (type == Event.Type.START_RECORD && "".equals(name)) {
                result = null; // Ignore missing record ID
            }
            else {
                result = Mismatch.NAME;
            }
        }
        else if (value != null && !value.equals(event.getValue())) {
            result = Mismatch.VALUE;
        }
        else {
            result = null;
        }

        return result;
    }

    private void incrementPosition() {
        if (mParent == null || mEvent.getName().endsWith(JsonEncoder.ARRAY_MARKER)) {
            ++mPosition;
        }
    }

    private void appendPosition(final StringBuilder aBuilder) {
        if (mPosition > 0) {
            aBuilder
                .append("<")
                .append(mPosition)
                .append(">");
        }
    }

}
