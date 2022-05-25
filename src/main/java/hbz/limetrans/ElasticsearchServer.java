package hbz.limetrans;

import hbz.limetrans.util.LimetransException;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class ElasticsearchServer {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final Map<String, ElasticsearchServer> CACHE = new HashMap<>();

    private final File mTempDir;
    private final Node mNode;
    private final String mDataDir;

    private ElasticsearchServer(final String aDataDir, final File aTempDir) {
        mDataDir = aDataDir;
        mTempDir = aTempDir;

        mNode = NodeBuilder.nodeBuilder()
            .settings(Settings.settingsBuilder()
                    .put("http.enabled", false)
                    .put("path.home", mDataDir)
                    .build())
            .local(true)
            .node();
    }

    public static Client getClient(final String aDataDir, final Consumer<ElasticsearchServer> aConsumer) {
        final File tempDir;
        final String dataDir;

        if (aDataDir == null) {
            try {
                tempDir = Files.createTempDirectory("elasticsearch").toFile();
                dataDir = tempDir.getPath();
            }
            catch (final IOException e) {
                throw new LimetransException("Failed to create directory", e);
            }
        }
        else {
            tempDir = null;
            dataDir = aDataDir;
        }

        final ElasticsearchServer server;
        if (CACHE.containsKey(dataDir)) {
            LOGGER.info("Accessing embedded server: {}", aDataDir);
            server = CACHE.get(dataDir);
        }
        else {
            LOGGER.info("Starting embedded server: {}", aDataDir);
            server = new ElasticsearchServer(dataDir, tempDir);

            CACHE.put(dataDir, server);

            if (aConsumer != null) {
                aConsumer.accept(server);
            }
        }

        return server.getClient();
    }

    private Client getClient() {
        return mNode.client();
    }

    public void shutdown() {
        shutdown(false);
    }

    public void shutdown(final boolean aDeleteOnExit) {
        LOGGER.info("Shutting down embedded server: {}", mDataDir);

        CACHE.remove(mDataDir);
        mNode.close();

        if (mTempDir != null) {
            deleteDirectory(mTempDir);
        }
        else if (aDeleteOnExit) {
            deleteDirectory(new File(mDataDir));
        }
    }

    private void deleteDirectory(final File aDirectory) {
        try {
            FileUtils.deleteDirectory(aDirectory);
        }
        catch (final IOException e) {
            throw new LimetransException("Failed to delete directory", e);
        }
    }

}
