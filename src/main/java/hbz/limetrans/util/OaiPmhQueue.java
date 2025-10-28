package hbz.limetrans.util;

import org.metafacture.biblio.OaiPmhOpener;
import org.metafacture.biblio.marc21.MarcXmlHandler;
import org.metafacture.framework.LifeCycle;
import org.metafacture.framework.Sender;
import org.metafacture.framework.StreamReceiver;
import org.metafacture.xml.XmlDecoder;

public class OaiPmhQueue extends AbstractInputQueue {

    private final String mMetadataPrefix;
    private final String mUri;

    public OaiPmhQueue(final Settings aSettings) {
        init(aSettings);

        mMetadataPrefix = aSettings.get("metadataPrefix", "marcxml");
        mUri = aSettings.get("uri");
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
            process("OAI-PMH URI: " + mUri, opener, mUri);
        }

        getLogger().info("Finished processing OAI-PMH URI");

        return opener;
    }

}
