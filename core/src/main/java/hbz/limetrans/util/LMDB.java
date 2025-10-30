package hbz.limetrans.util;

import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;
import org.lmdbjava.EnvInfo;
import org.lmdbjava.Stat;
import org.lmdbjava.Txn;
import org.metafacture.io.FileCompression;
import org.metafacture.metamorph.api.helpers.AbstractReadOnlyMap;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Objects;
import java.util.function.Function;

public final class LMDB extends AbstractReadOnlyMap<String, String> implements Closeable {

    private static final int MAP_SIZE = 10_000; // * 1048576
    private static final int MAX_VALUE_SIZE = 1024 * MAP_SIZE;

    private static final String EXTENSION = ".lmdb";
    private static final String SEPARATOR = "\u001D";

    private static final Charset CHARSET = Charset.defaultCharset();

    private final ByteBuffer mKeyBuffer;
    private final ByteBuffer mValueBuffer;
    private final Dbi<ByteBuffer> mDbi;
    private final Env<ByteBuffer> mEnv;
    private final File mTempFile;
    private final Txn<ByteBuffer> mTxn;

    public LMDB(final String aPath) {
        final File file = new File(aPath);
        final boolean readonly = file.exists();

        final File realFile;

        if (aPath.endsWith(EXTENSION)) {
            mTempFile = null;
            realFile = file;
        }
        else if (readonly) {
            try {
                final Path tempFile = Files.createTempFile(file.getName(), EXTENSION);

                mTempFile = tempFile.toFile();
                mTempFile.deleteOnExit();

                realFile = mTempFile;

                try (
                    InputStream stream = new FileInputStream(aPath);
                    InputStream decompressor = FileCompression.AUTO.createDecompressor(stream, true)
                ) {
                    Files.copy(decompressor, tempFile, StandardCopyOption.REPLACE_EXISTING);
                }
            }
            catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        else {
            throw new IllegalArgumentException("Writing to compressed file not supported (yet).");
        }

        mEnv = Env.open(realFile,
                readonly ? 0 : MAP_SIZE,
                EnvFlags.MDB_NOLOCK,
                EnvFlags.MDB_NOSUBDIR,
                readonly ? EnvFlags.MDB_RDONLY_ENV : null);

        mDbi = mEnv.openDbi((byte[]) null);
        mTxn = readonly ? mEnv.txnRead() : mEnv.txnWrite();

        mKeyBuffer = ByteBuffer.allocateDirect(mEnv.getMaxKeySize());
        mValueBuffer = ByteBuffer.allocateDirect(MAX_VALUE_SIZE);
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
            mTxn.commit();
            mTxn.close();
        }
        finally {
            mEnv.close();

            if (mTempFile != null) {
                mTempFile.delete();
            }
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

        final String key = aKey.toString();
        if (key.isEmpty()) {
            return aDefault;
        }

        try {
            final ByteBuffer val = withBuffer(mKeyBuffer, key, k -> mDbi.get(mTxn, k));
            return val != null ? CHARSET.decode(val).toString() : aDefault;
        }
        catch (final Dbi.BadValueSizeException e) {
            throw new RuntimeException("Failed to get " + aKey, e);
        }
    }

    public String putKV(final String aKey, final String aValue) {
        try {
            withBuffer(mKeyBuffer, aKey, k ->
                    withBuffer(mValueBuffer, aValue, v ->
                            mDbi.put(mTxn, k, v)));
        }
        catch (final BufferOverflowException e) {
            throw new RuntimeException("Failed to add " + aKey + "=" + aValue, e);
        }

        return null;
    }

    private <T> T withBuffer(final ByteBuffer aBuffer, final String aString, final Function<ByteBuffer, T> aFunction) {
        aBuffer.put(Objects.requireNonNull(aString).getBytes(CHARSET)).flip();
        final T result = aFunction.apply(aBuffer);

        aBuffer.clear();
        return result;
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
                try (
                    Reader reader = new InputStreamReader(System.in);
                    BufferedReader bufferedReader = new BufferedReader(reader)
                ) {
                    bufferedReader.lines().forEach(l -> {
                        final String[] parts = l.split(SEPARATOR, 2);
                        if (parts.length == 2) {
                            lmdb.putKV(parts[0], parts[1]);
                        }
                    });
                }
                catch (final IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }

}
