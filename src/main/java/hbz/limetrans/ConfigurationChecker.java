package hbz.limetrans;

import org.apache.commons.validator.routines.UrlValidator;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

public class ConfigurationChecker {

    final private static String[] protocols = new String[] {"http", "https", "ftp", "file"};

    public static URL getConfigUrlFrom(String[] aArgs) throws MalformedURLException {

        // check args[] size
        if (aArgs.length < 1){
            throw new IllegalArgumentException("Could not process limetrans: configuration missing.");
        }
        if (aArgs.length > 1){
            throw new IllegalArgumentException("Could not process limetrans: too many arguments: ".concat(aArgs.toString()));
        }

        // trim args[0]
        String trimmed = aArgs[0].trim();
        if (trimmed.isEmpty()){
            throw new IllegalArgumentException("Could not process limetrans: empty configuration argument.");
        }

        // check trimmed
        if (new UrlValidator(protocols).isValid(trimmed)){
            return new URL(trimmed);
        }
        File file = new File(trimmed);
        if (!file.exists()){
            throw new IllegalArgumentException("Could not process limetrans: invalid configuration argument: ".concat(trimmed));
        }
        else{
            return file.toURI().toURL();
        }
    }
}
