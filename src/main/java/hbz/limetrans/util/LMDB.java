package hbz.limetrans.util;

import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;
import org.lmdbjava.EnvInfo;
import org.lmdbjava.Stat;
import org.lmdbjava.Txn;
import org.metafacture.metamorph.api.helpers.AbstractReadOnlyMap;

import java.io.Closeable;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public final class LMDB extends AbstractReadOnlyMap<String, String> implements AutoCloseable, Closeable {

    private static final int SIZE = 10_000;

    private static final Charset CHARSET = Charset.defaultCharset();

    private final ByteBuffer mKey;
    private final Dbi<ByteBuffer> mDbi;
    private final Env<ByteBuffer> mEnv;
    private final Txn<ByteBuffer> mTxn;

    public LMDB(final String aPath) {
        final File file = new File(aPath);
        final boolean readonly = file.exists();

        mEnv = Env.open(file,
                readonly ? 0 : SIZE,
                EnvFlags.MDB_NOLOCK,
                EnvFlags.MDB_NOSUBDIR,
                readonly ? EnvFlags.MDB_RDONLY_ENV : null);

        mDbi = mEnv.openDbi((byte[]) null);
        mKey = ByteBuffer.allocateDirect(mEnv.getMaxKeySize());
        mTxn = mEnv.txnRead();
    }

    public boolean isReadOnly() {
        return mEnv.isReadOnly();
    }

    public boolean isClosed() {
        return mEnv.isClosed();
    }

    public EnvInfo info() {
        return mEnv.info();
    }

    public Stat stat() {
        return mEnv.stat();
    }

    public long count() {
        return stat().entries;
    }

    @Override
    public void close() {
        try {
            mTxn.close();
        }
        finally {
            mEnv.close();
        }
    }

    @Override
    public String get(final Object aKey) {
        return getOrDefault(aKey, null);
    }

    @Override
    public String getOrDefault(final Object aKey, final String aDefault) {
        if (aKey == null) {
            return aDefault;
        }

        mKey.put(aKey.toString().getBytes(CHARSET)).flip();
        final ByteBuffer val = mDbi.get(mTxn, mKey);
        mKey.clear();

        return val != null ? CHARSET.decode(val).toString() : aDefault;
    }

    public static void main(final String[] aArgs) {
        final int argc = aArgs.length;
        if (argc < 1) {
            throw new IllegalArgumentException("Usage: " + LMDB.class + " <path> [<key>...]");
        }

        try (LMDB lmdb = new LMDB(aArgs[0])) {
            if (lmdb.isReadOnly()) {
                System.out.println(lmdb.info());
                System.out.println(lmdb.stat());

                for (int i = 1; i < argc; ++i) {
                    System.out.println(aArgs[i] + "=" + lmdb.get(aArgs[i]));
                }
            }
            else {
                System.err.println("Writing not supported (yet).");
            }
        }
    }

}
