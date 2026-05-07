package hbz.limetrans.util;

import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.samtools.util.BlockCompressedStreamConstants;

import java.io.File;
import java.io.IOException;

public final class BGZF {

    private BGZF() {
        throw new IllegalAccessError("Utility class");
    }

    public static void main(final String[] aArgs) throws IOException {
        final int argc = aArgs.length;
        if (argc != 1 && argc != 3) { // checkstyle-disable-line MagicNumber
            throw new IllegalArgumentException("Usage: " + BGZF.class + " <path> [<offset> <length>]");
        }

        try (BlockCompressedInputStream bgzf = new BlockCompressedInputStream(new File(aArgs[0]))) {
            if (argc == 1) {
                int read = 0;
                long total = 0;

                final byte[] buffer = new byte[BlockCompressedStreamConstants.MAX_COMPRESSED_BLOCK_SIZE];

                while ((read = bgzf.read(buffer)) != -1) {
                    total += read;
                }

                System.out.println(aArgs[0] + "=" + total);
            }
            else {
                final byte[] buffer = new byte[Integer.parseInt(aArgs[2])];

                bgzf.seek(Long.parseLong(aArgs[1]));

                if (bgzf.read(buffer) != -1) {
                    System.out.println(new String(buffer));
                }
            }
        }
    }

}
