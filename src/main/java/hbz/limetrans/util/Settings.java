package hbz.limetrans.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Settings {

    private static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("(?<!\\\\)\\$\\{(.*?)\\}");

    private final Map<String, Object> mMap = new HashMap<>();

    private Settings(final Map<String, Object> aMap) {
        if (aMap != null) {
            mMap.putAll(aMap);
        }
    }

    private Map<String, Object> getMap() {
        return mMap;
    }

    public static Builder settingsBuilder() {
        return new Builder();
    }

    public Settings amendSettings(final BiConsumer<Settings, Settings.Builder> aConsumer) {
        final Builder builder = new Builder();

        builder.mMap.putAll(mMap); // TODO: deep clone?
        aConsumer.accept(this, builder);

        return builder.build();
    }

    @Override
    public String toString() {
        return mMap.toString();
    }

    public String toJson() throws IOException {
        return Builder.MAPPER.writeValueAsString(mMap);
    }

    public void forEach(final BiConsumer<Settings, String> aConsumer) {
        mMap.keySet().forEach(k -> aConsumer.accept(this, k));
    }

    public Set<Map.Entry<String, Settings>> entrySet(final String aSetting) {
        final Map<String, Settings> result = new HashMap<>();

        getAsSettings(aSetting).forEach((s, k) ->
                result.put(k, s.getAsSettings(k)));

        return result.entrySet();
    }

    public boolean containsSetting(final String aSetting) {
        return mMap.containsKey(aSetting);
    }

    public String get(final String aSetting) {
        return get(aSetting, null);
    }

    public String get(final String aSetting, final String aDefaultValue) {
        final Object value = getSetting(aSetting, aDefaultValue);

        if (value == null || value instanceof String) {
            return (String) value;
        }

        throw new UnexpectedValue(String.class, value.getClass(), aSetting);
    }

    public Integer getAsInt(final String aSetting, final Integer aDefaultValue) {
        final Object value = getSetting(aSetting, aDefaultValue);

        if (value == null || value instanceof Integer) {
            return (Integer) value;
        }

        throw new UnexpectedValue(Integer.class, value.getClass(), aSetting);
    }

    public Boolean getAsBoolean(final String aSetting, final Boolean aDefaultValue) {
        final Object value = getSetting(aSetting, aDefaultValue);

        if (value == null || value instanceof Boolean) {
            return (Boolean) value;
        }

        throw new UnexpectedValue(Boolean.class, value.getClass(), aSetting);
    }

    public String[] getAsArray(final String aSetting, final String... aDefaultValue) {
        final Object value = getSetting(aSetting, null);

        if (value == null) {
            return aDefaultValue;
        }
        else if (value instanceof List) {
            try {
                @SuppressWarnings("unchecked")
                final List<String> listValue = (List) value;

                return listValue.toArray(new String[listValue.size()]);
            }
            catch (final ClassCastException e) {
                // throw UnexpectedValue below
            }
        }
        else if (value instanceof String) {
            return new String[]{(String) value};
        }

        throw new UnexpectedValue(List.class, value.getClass(), aSetting);
    }

    public List<String> getAsList(final String aSetting) {
        return Arrays.asList(getAsArray(aSetting));
    }

    public Settings getAsSettings(final String aSetting) {
        return new Settings(getSettingMap(mMap, aSetting));
    }

    private static Map<String, Object> getSettingMap(final Map<String, Object> aMap, final String aSetting) {
        final Object value = aMap.get(aSetting);

        if (value == null) {
            return null;
        }
        else if (value instanceof Map) {
            try {
                @SuppressWarnings("unchecked")
                final Map<String, Object> mapValue = (Map) value;

                return mapValue;
            }
            catch (final ClassCastException e) {
                // throw UnexpectedValue below
            }
        }

        throw new UnexpectedValue(Map.class, value.getClass(), aSetting);
    }

    public Map<String, String> getAsFlatMap(final String aSeparator) {
        final Map<String, String> result = new HashMap<>();
        flattenMap(result, mMap, "", aSeparator);
        return result;
    }

    private void flattenMap(final Map<String, String> aResult, final Map<String, Object> aMap, final String aSetting, final String aSeparator) {
        for (final Map.Entry<String, Object> entry : aMap.entrySet()) {
            final String key = aSetting + entry.getKey();
            final Object value = entry.getValue();

            if (value instanceof Map) {
                try {
                    @SuppressWarnings("unchecked")
                    final Map<String, Object> mapValue = (Map) value;

                    flattenMap(aResult, mapValue, key + aSeparator, aSeparator);
                }
                catch (final ClassCastException e) {
                    throw new UnexpectedValue(Map.class, value.getClass(), key);
                }
            }
            else if (value instanceof List) {
                try {
                    @SuppressWarnings("unchecked")
                    final List<String> listValue = (List) value;

                    int counter = -1;

                    for (final String v : listValue) {
                        aResult.put(key + aSeparator + (++counter), v);
                    }
                }
                catch (final ClassCastException e) {
                    throw new UnexpectedValue(List.class, value.getClass(), key);
                }
            }
            else if (value instanceof String) {
                aResult.put(key, (String) value);
            }
            else if (value != null) {
                aResult.put(key, String.valueOf(value));
            }
        }
    }

    private Object getSetting(final String aSetting, final Object aDefaultValue) {
        return mMap.getOrDefault(aSetting, aDefaultValue);
    }

    public static class Builder {

        private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(JsonParser.Feature.ALLOW_COMMENTS, true)
            .configure(JsonParser.Feature.STRICT_DUPLICATE_DETECTION, true);

        private final Map<String, Object> mMap = new HashMap<>();

        private Builder() {
        }

        public Settings build() {
            return new Settings(mMap);
        }

        private void mergeMap(final Map<String, Object> aOldMap, final Map<String, Object> aNewMap) {
            aNewMap.forEach((k, v) -> aOldMap.merge(k, v, (o, n) -> {
                if (o instanceof Map && n instanceof Map) {
                    @SuppressWarnings("unchecked")
                    final Map<String, Object> oldMap = (Map) o;

                    @SuppressWarnings("unchecked")
                    final Map<String, Object> newMap = (Map) n;

                    mergeMap(oldMap, newMap);
                    return oldMap;
                }
                else {
                    return n;
                }
            }));
        }

        public Builder load(final Settings aSettings) {
            mergeMap(mMap, aSettings.getMap());
            return this;
        }

        public Builder load(final InputStream aIn) throws IOException {
            final Object value = MAPPER.readValue(aIn, Map.class);

            if (value instanceof Map) {
                try {
                    @SuppressWarnings("unchecked")
                    final Map<String, Object> mapValue = (Map) value;

                    mMap.putAll(mapValue);
                    replacePlaceholders(mMap);

                    return this;
                }
                catch (final ClassCastException e) {
                    // throw UnexpectedValue below
                }
            }

            throw new UnexpectedValue("load", Map.class, value.getClass());
        }

        private void replacePlaceholders(final Map<String, Object> aMap) {
            aMap.entrySet().forEach(entry -> {
                final Object value = entry.getValue();

                if (value instanceof String) {
                    @SuppressWarnings("unchecked")
                    final String string = (String) value;

                    entry.setValue(replacePlaceholders(string));
                }
                else if (value instanceof List) {
                    @SuppressWarnings("unchecked")
                    final List<String> listValue = (List) value;

                    listValue.replaceAll(this::replacePlaceholders);
                }
                else if (value instanceof Map) {
                    @SuppressWarnings("unchecked")
                    final Map<String, Object> mapValue = (Map) value;

                    replacePlaceholders(mapValue);
                }
            });
        }

        private String replacePlaceholders(final String aString) {
            final Matcher matcher = PLACEHOLDER_PATTERN.matcher(aString);
            final StringBuffer sb = new StringBuffer();

            while (matcher.find()) {
                final String replacement = getReplacement(mMap, matcher.group(1));
                matcher.appendReplacement(sb, Matcher.quoteReplacement(replacePlaceholders(replacement)));
            }

            return matcher.appendTail(sb).toString();
        }

        private String getReplacement(final Map<String, Object> aMap, final String aKey) {
            if (aMap.containsKey(aKey)) {
                final Object value = aMap.get(aKey);

                if (value instanceof String) {
                    @SuppressWarnings("unchecked")
                    final String string = (String) value;

                    return string;
                }
                else if (value instanceof List) {
                    @SuppressWarnings("unchecked")
                    final List<String> listValue = (List) value;

                    return listValue.get(0);
                }
            }

            throw new IllegalArgumentException("Could not resolve placeholder '" + aKey + "'");
        }

        public Builder put(final String aSetting, final String aValue) {
            return putSetting(new String[]{aSetting}, aValue);
        }

        public Builder put(final String[] aPath, final Boolean aValue) {
            return putSetting(aPath, aValue);
        }

        public Builder put(final String[] aPath, final String aValue) {
            return putSetting(aPath, aValue);
        }

        public Builder put(final String[] aPath, final Integer aValue) {
            return putSetting(aPath, aValue);
        }

        public Builder put(final String[] aPath, final String[] aValue) {
            return putSetting(aPath, Arrays.asList(aValue));
        }

        private Builder putSetting(final String[] aPath, final Object aValue) {
            final int last = aPath.length - 1;

            Map<String, Object> result = mMap;

            for (int i = 0; i < last; ++i) {
                final String aSetting = aPath[i];

                Map<String, Object> mapValue = getSettingMap(result, aSetting);

                if (mapValue == null) {
                    mapValue = new HashMap<>();
                    result.put(aSetting, mapValue);
                }

                result = mapValue;
            }

            result.put(aPath[last], aValue);

            return this;
        }

        public Object getSetting(final String[] aPath) {
            final int last = aPath.length - 1;

            Map<String, Object> result = mMap;

            for (int i = 0; i < last; ++i) {
                final Map<String, Object> mapValue = getSettingMap(result, aPath[i]);

                if (mapValue == null) {
                    return null;
                }

                result = mapValue;
            }

            return result.get(aPath[last]);
        }

    }

    public static class UnexpectedValue extends RuntimeException {

        private UnexpectedValue(final String aAction, final Class<?> aExpected, final Class<?> aActual) {
            super("Failed to " + aAction + "; expected " + aExpected + ", got " + aActual);
        }

        private UnexpectedValue(final Class<?> aExpected, final Class<?> aActual, final String aSetting) {
            this("get " + aSetting, aExpected, aActual);
        }

    }

}
