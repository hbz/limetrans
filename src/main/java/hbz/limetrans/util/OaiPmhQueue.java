package hbz.limetrans.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.metafacture.biblio.OaiPmhOpener;
import org.metafacture.biblio.marc21.MarcXmlHandler;
import org.metafacture.framework.LifeCycle;
import org.metafacture.framework.Sender;
import org.metafacture.framework.StreamReceiver;
import org.metafacture.xml.XmlDecoder;

public class OaiPmhQueue implements InputQueue {

    private static final Logger LOGGER = LogManager.getLogger();

    private final String mMetadataPrefix;
    private final String mUri;

    public OaiPmhQueue(final Settings aSettings) {
        mMetadataPrefix = aSettings.get("metadataPrefix", "marcxml");
        mUri = aSettings.get("uri");
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public int size() {
        return mUri == null ? 0 : 1;
    }

    @Override
    public <T extends StreamReceiver & Sender<StreamReceiver>> LifeCycle process(final StreamReceiver aReceiver, final T aSender) {
        final OaiPmhOpener opener = new OaiPmhOpener();
        opener.setMetadataPrefix(mMetadataPrefix);

        Sender<StreamReceiver> result = opener
            .setReceiver(new XmlDecoder())
            .setReceiver(new MarcXmlHandler());

        if (aSender != null) {
            result = result.setReceiver(aSender);
        }

        result.setReceiver(aReceiver);

        if (!isEmpty()) {
            LOGGER.info("Processing OAI-PMH URI: " + mUri);

            try {
                opener.process(mUri);
            }
            catch (final Exception e) { // checkstyle-disable-line IllegalCatch
                LOGGER.error("Processing failed:", e);
            }
        }

        LOGGER.info("Finished processing OAI-PMH URI");

        return opener;
    }

}
