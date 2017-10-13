package hbz.limetrans.test;

import org.metafacture.javaintegration.EventList.Event;

import static org.metafacture.json.JsonEncoder.ARRAY_MARKER;

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

    private int mPosition = 0;

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

        final String name = mEvent.getName();
        final String value = mEvent.getValue();

        if (mEvent.getType() != event.getType()) {
            return Mismatch.TYPE;
        }
        else if (name != null && !name.equals(event.getName())) {
            return Mismatch.NAME;
        }
        else if (value != null && !value.equals(event.getValue())) {
            return Mismatch.VALUE;
        }
        else {
            return null;
        }
    }

    private void incrementPosition() {
        if (mParent == null || mEvent.getName().endsWith(ARRAY_MARKER)) {
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
