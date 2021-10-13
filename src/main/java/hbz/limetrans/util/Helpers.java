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
import java.nio.file.StandardCopyOption;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

public class Helpers {

    public static final String GROUP_PREFIX = "hbz.limetrans.";

    public static final String CLASSPATH_PREFIX = "classpath:";

    private static final TransformerFactory TRANSFORMER_FACTORY = TransformerFactory.newInstance();
    private static final String INDENT_AMOUNT_KEY = "{http://xml.apache.org/xslt}indent-amount";

    private Helpers() {
        throw new IllegalAccessError("Utility class");
    }

    public static String getProperty(final String aKey) {
        return System.getProperty(GROUP_PREFIX + aKey);
    }

    public static String getProperty(final String aKey, final String aDefaultValue) {
        return getProperty(aKey, Function.identity(), aDefaultValue);
    }

    public static boolean getProperty(final String aKey, final boolean aDefaultValue) {
        final Boolean value = getProperty(aKey, Boolean::valueOf, aDefaultValue);
        return value != null ? value.booleanValue() : aDefaultValue;
    }

    public static <T> T getProperty(final String aKey, final Function<String, T> aFunction, final T aDefaultValue) {
        final String value = getProperty(aKey);
        return value != null ? value.isEmpty() ? aDefaultValue : aFunction.apply(value) : null;
    }

    public static <T extends Enum<T>> T getEnumProperty(final String aKey, final String aDefaultPropValue, final T aDefaultEnumValue, final Consumer<String> aConsumer, final UnaryOperator<String> aOperator) {
        final String propValue = getProperty(aKey, aDefaultPropValue);

        if (propValue == null) {
            if (aConsumer != null) {
                aConsumer.accept("Missing " + aKey + " property; using default: " + aDefaultEnumValue);
            }

            return aDefaultEnumValue;
        }
        else {
            try {
                return Enum.valueOf(aDefaultEnumValue.getDeclaringClass(), aOperator != null ? aOperator.apply(propValue) : propValue);
            }
            catch (final IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid " + aKey + " property: " + propValue);
            }
        }
    }

    public static Settings loadSettings(final String aPath) throws IOException {
        return loadSettings(aPath, null);
    }

    public static Settings loadSettings(final String aPath, final Consumer<Settings.Builder> aConsumer) throws IOException {
        final Settings.Builder settingsBuilder = Settings.settingsBuilder();

        if (aPath != null) {
            try (InputStream in = new FileInputStream(aPath)) {
                settingsBuilder.load(in);
            }
        }

        if (aConsumer != null) {
            aConsumer.accept(settingsBuilder);
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

    public static void updateTestFile(final String aTarget, final Supplier<String> aSupplier) throws IOException {
        if (aTarget != null && getProperty("updateTestFiles", false)) {
            final String source = aSupplier.get();
            if (source != null) {
                Files.move(Paths.get(source), Paths.get(aTarget), StandardCopyOption.REPLACE_EXISTING);
            }
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
