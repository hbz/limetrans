package hbz.limetrans;

import org.apache.commons.io.FileUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class ElasticsearchServer {

    private final File mTempDir;
    private final Node mNode;
    private final String mDataDir;

    public ElasticsearchServer(final String aDataDir) {
        if (aDataDir == null) {
            try {
                mTempDir = Files.createTempDirectory("elasticsearch").toFile();
                mDataDir = mTempDir.getPath();
            }
            catch (final IOException e) {
                throw new RuntimeException("Failed to create temporary directory", e);
            }
        }
        else {
            mTempDir = null;
            mDataDir = aDataDir;
        }

        mNode = NodeBuilder.nodeBuilder()
            .settings(Settings.settingsBuilder()
                    .put("http.enabled", false)
                    .put("path.home", mDataDir)
                    .build())
            .local(true)
            .node();
    }

    public Client getClient() {
        return mNode.client();
    }

    public void shutdown() {
        mNode.close();

        if (mTempDir != null) {
            try {
                FileUtils.deleteDirectory(mTempDir);
            }
            catch (final IOException e) {
                throw new RuntimeException("Failed to delete temporary directory", e);
            }
        }
    }

}
