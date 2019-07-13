package client;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;


public class ConfigReader {
    private static final Properties configFile = new Properties();

    public static void readConfigs(String propFilePath) throws Exception {
       
        try {
            InputStream in = new FileInputStream(propFilePath);
            configFile.load(in);
        } catch (Exception ioe) {
            throw new Exception("Error while loading config file :" + ioe.getMessage());
        }
    }

    public static Properties getConfigFile() {
        return configFile;
    }
}
