package hbz.limetrans.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

public class Helpers {

    public static final String CLASSPATH_PREFIX = "classpath:";

    private static final TransformerFactory TRANSFORMER_FACTORY = TransformerFactory.newInstance();
    private static final String INDENT_AMOUNT_KEY = "{http://xml.apache.org/xslt}indent-amount";

    private Helpers() {
        throw new IllegalAccessError("Utility class");
    }

    public static Settings loadSettings(final String aPath) throws IOException {
        final Settings.Builder settingsBuilder = Settings.settingsBuilder();

        if (aPath != null) {
            try (InputStream in = new FileInputStream(aPath)) {
                settingsBuilder.load(in);
            }
        }

        return settingsBuilder.build();
    }

    public static String slurpFile(final String aPath) throws IOException {
        return new String(Files.readAllBytes(Paths.get(aPath)));
    }

    public static String getResourcePath(final Class<?> aClass, final String aPath) throws IOException {
        final URL url = aClass.getResource(aPath);
        if (url == null) {
            throw new FileNotFoundException("Resource not found for " + aClass.toString() + ": " + aPath);
        }
        else {
            return url.getPath();
        }
    }

    public static String getPath(final Class<?> aClass, final String aPath) throws IOException {
        if (aPath != null && aPath.startsWith(CLASSPATH_PREFIX)) {
            return getResourcePath(aClass, aPath.substring(CLASSPATH_PREFIX.length()));
        }
        else {
            return aPath;
        }
    }

    public static String prettyXml(final String aString) {
        try (Reader reader = new StringReader(aString);
                Writer writer = new StringWriter()) {
            final Transformer transformer = TRANSFORMER_FACTORY.newTransformer();

            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.setOutputProperty(INDENT_AMOUNT_KEY, "2");
            transformer.transform(new StreamSource(reader), new StreamResult(writer));

            return writer.toString();
        }
        catch (final IOException | TransformerException e) {
            System.err.println(e);
            return aString;
        }
    }

}
