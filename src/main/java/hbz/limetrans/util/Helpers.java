package hbz.limetrans.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

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

}
