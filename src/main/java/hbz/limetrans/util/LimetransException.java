package hbz.limetrans.util;

public class LimetransException extends RuntimeException {

    private static final long serialVersionUID = -97804944829183531L;

    public LimetransException(final String message) {
        super(message);
    }

    public LimetransException(final Throwable cause) {
        super(cause);
    }

    public LimetransException(final String message, final Throwable cause) {
        super(message, cause);
    }

}
