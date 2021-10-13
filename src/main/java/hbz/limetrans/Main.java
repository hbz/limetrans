package hbz.limetrans;

import hbz.limetrans.util.Helpers;
import hbz.limetrans.util.Settings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Main {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final String[] INDEX_SETTING = new String[]{"output", "elasticsearch", "index"};

    private static final Pattern INDEX_NAME_PATTERN = Pattern.compile("([^-]+-[^-]+-)[^-]+-?(.*)");

    private enum Env {

        prod(settingsBuilder -> {
            setCluster(settingsBuilder, "hap");
            setHost(settingsBuilder, "hera.hbz-nrw.de:9300", "athene.hbz-nrw.de:9300", "persephone.hbz-nrw.de:9300");
        }),

        dev(settingsBuilder -> {
            setCluster(settingsBuilder, "zbn");
            setHost(settingsBuilder, "zephyros.hbz-nrw.de:9300", "boreas.hbz-nrw.de:9300", "notos.hbz-nrw.de:9300");
        }),

        d7test(settingsBuilder -> {
            setCluster(settingsBuilder, "zbn");
            setHost(settingsBuilder, "zephyros.hbz-nrw.de:9300", "boreas.hbz-nrw.de:9300", "notos.hbz-nrw.de:9300");
            setMaxAge(settingsBuilder, -1);

            final VarargsOperator<String> indexSetting = k -> {
                final String[] result = Arrays.copyOf(INDEX_SETTING, INDEX_SETTING.length + k.length);
                System.arraycopy(k, 0, result, INDEX_SETTING.length, k.length);
                return result;
            };

            if (settingsBuilder.getSetting(indexSetting.apply("name")) instanceof final String indexName) {
                final Matcher matcher = INDEX_NAME_PATTERN.matcher(indexName);
                if (!matcher.matches()) {
                    throw new RuntimeException("Invalid index name: " + indexName);
                }

                settingsBuilder.put(indexSetting.apply("name"), matcher.group(1) + "d7test-" + matcher.group(2));
                settingsBuilder.put(indexSetting.apply("timewindow"), "");
                settingsBuilder.put(indexSetting.apply("settings-inline", "index", "number_of_replicas"), 0);
            }
            else {
                LOGGER.warn("Missing index name setting.");
            }
        }),

        local(settingsBuilder -> {
            setCluster(settingsBuilder, "elasticsearch");
            setHost(settingsBuilder, "localhost:9300");
            setMaxAge(settingsBuilder, -1);
        }),

        ignore(settingsBuilder -> {});

        private final Consumer<Settings.Builder> mConsumer;

        Env(final Consumer<Settings.Builder> aConsumer) {
            mConsumer = aConsumer;
        }

        public Settings settings(final String aConf) {
            try {
                return Helpers.loadSettings(aConf, settingsBuilder -> {
                    settingsBuilder.put("env", toString());
                    mConsumer.accept(settingsBuilder);
                });
            }
            catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }

        private static void setCluster(final Settings.Builder aSettingsBuilder, final String aCluster) {
            aSettingsBuilder.put(new String[]{"output", "elasticsearch", "cluster"}, aCluster);
        }

        private static void setHost(final Settings.Builder aSettingsBuilder, final String... aHost) {
            aSettingsBuilder.put(new String[]{"output", "elasticsearch", "host"}, aHost);
        }

        private static void setMaxAge(final Settings.Builder aSettingsBuilder, final int aMaxAge) {
            aSettingsBuilder.put(new String[]{"input", "queue", "max-age"}, aMaxAge);
        }

    }

    private Main() {
        throw new IllegalAccessError("Utility class");
    }

    public static void main(final String[] aArgs) throws IOException {
        new Limetrans(setup(aArgs)).process();
    }

    private static Settings setup(final String[] aArgs) throws IOException {
        if (aArgs.length < 1) {
            throw new IllegalArgumentException("Could not process limetrans: configuration missing.");
        }

        if (aArgs.length > 1) {
            throw new IllegalArgumentException("Could not process limetrans: too many arguments: " + Arrays.toString(aArgs));
        }

        final String arg = aArgs[0].trim();
        if (arg.isEmpty()) {
            throw new IllegalArgumentException("Could not process limetrans: empty configuration argument.");
        }

        return Helpers.getEnumProperty("env", null, Env.ignore, LOGGER::info, null).settings(arg);
    }

    private interface VarargsOperator<T> {
        @SuppressWarnings("unchecked")
        T[] apply(T... aArgs);
    }

}
