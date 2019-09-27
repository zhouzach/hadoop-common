package impala;

import org.apache.hadoop.conf.Configuration;

public class KbsConfiguration extends Configuration {

    public static KbsConfiguration newInstance() {
        return new KbsConfiguration();
    }

    public Configuration setPro(String name, String value) {
        super.set(name, value);
        return this;
    }

}
