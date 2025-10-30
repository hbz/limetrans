package hbz.limetrans.util;

public class LimetransException extends RuntimeException {

    private static final long serialVersionUID = -97804944829183531L;

    public LimetransException(final String aMessage) {
        super(aMessage);
    }

    public LimetransException(final Throwable aCause) {
        super(aCause);
    }

    public LimetransException(final String aMessage, final Throwable aCause) {
        super(aMessage, aCause);
    }

}
