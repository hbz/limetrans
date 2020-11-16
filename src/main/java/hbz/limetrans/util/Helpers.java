package hbz.limetrans.util;

import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;

public class Helpers {

    public static final String CLASSPATH_PREFIX = "classpath:";

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

    public static String slurpFile(final URL aUrl) throws IOException {
        return IOUtils.toString(aUrl, Charset.defaultCharset());
    }

    public static String slurpFile(final String aPath) throws IOException {
        return slurpFile(new File(aPath).toURI().toURL());
    }

    public static String slurpFile(final String aPath, final Class<?> aClass) throws IOException {
        final URL url = getClasspathUrl(aClass, aPath);
        return url != null ? slurpFile(url) : slurpFile(aPath);
    }

    public static String getPath(final String aPath, final Class<?> aClass) throws IOException {
        final URL url = getClasspathUrl(aClass, aPath);
        return url != null ? url.toString() : aPath;
    }

    public static URL getResourceUrl(final Class<?> aClass, final String aPath) throws IOException {
        final URL url = aClass.getResource(aPath);
        if (url == null) {
            throw new FileNotFoundException("Resource not found for " + aClass.toString() + ": " + aPath);
        }
        else {
            return url;
        }
    }

    public static URL getClasspathUrl(final Class<?> aClass, final String aPath) throws IOException {
        if (aPath != null && aPath.startsWith(CLASSPATH_PREFIX)) {
            return getResourceUrl(aClass, aPath.substring(CLASSPATH_PREFIX.length()));
        }
        else {
            return null;
        }
    }

}
