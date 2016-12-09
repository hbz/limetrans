package hbz.limetrans.util;

import org.culturegraph.mf.stream.source.FileOpener;
import org.xbib.common.settings.Settings;
import org.xbib.util.Finder.PathFile;
import org.xbib.util.Finder;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

public class FileQueue {

    private final Queue<PathFile> mQueue = new LinkedList<>();

    public FileQueue(final Settings aSettings) throws IOException {
      put(aSettings);
    }

    public FileQueue(final String[] aFileNames) throws IOException {
        for (final String fileName : aFileNames) {
            final File file = new File(fileName);

            put(Settings.settingsBuilder()
                    .put("path", file.getParent())
                    .put("pattern", file.getName())
                    .build());
        }
    }

    public void process(final FileOpener opener) {
        for (final PathFile pathFile : mQueue) {
            opener.process(pathFile.toString());
        }
    }

    public boolean isEmpty() {
        return mQueue.isEmpty();
    }

    public int size() {
        return mQueue.size();
    }

    private void put(final Settings aSettings) throws IOException {
        if (aSettings == null) {
            return;
        }

        final String path = aSettings.get("path");
        final String pattern = aSettings.get("pattern");

        if (path == null || pattern == null) {
            return;
        }

        mQueue.addAll(new Finder().find(
                    aSettings.get("base"), aSettings.get("basepattern"), path, pattern)
                .sortBy(aSettings.get("sort_by", "lastmodified"))
                .order(aSettings.get("order", "asc"))
                .getPathFiles(aSettings.getAsInt("max", -1)));
    }

}
