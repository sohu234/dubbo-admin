package org.apache.dubbo.admin.registry.config.impl;

import org.apache.dubbo.admin.common.util.Constants;
import org.apache.dubbo.admin.registry.config.GovernanceConfiguration;
import org.apache.dubbo.admin.registry.metadata.impl.NacosMetaDataCollector;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.exception.NacosException;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.alibaba.nacos.api.PropertyKeyConst.SERVER_ADDR;

public class NacosConfiguration implements GovernanceConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(NacosMetaDataCollector.class);
    private Pattern pattern = Pattern.compile("config/(\\S*)/dubbo.properties");
    private ConfigService configService;
    private String group;
    private URL url;

    @Override
    public void init() {
        group = url.getParameter(Constants.GROUP_KEY, "DEFAULT_GROUP");

        configService = buildConfigService(url);
    }

    @Override
    public void setUrl(URL url) {
        this.url = url;
    }

    @Override
    public URL getUrl() {
        return this.url;
    }

    @Override
    public String setConfig(String key, String value) {
        return setConfig(group,key,value);
    }

    @Override
    public String getConfig(String key) {
        return getConfig(group,key);
    }

    @Override
    public boolean deleteConfig(String key) {
        return deleteConfig(group,key);
    }

    @Override
    public String setConfig(String group, String key, String value) {
        try {
            configService.publishConfig(wrapper(key),group,value);
        } catch (NacosException e) {
            logger.error(e.getErrMsg(),e);
        }
        return null;
    }

    @Override
    public String getConfig(String group, String key) {
        try {
            return configService.getConfig(wrapper(key),group,1000L);
        } catch (NacosException e) {
            logger.error(e.getErrMsg(),e);
        }
        return null;
    }

    @Override
    public boolean deleteConfig(String group, String key) {
        try {
            return configService.removeConfig(wrapper(key),group);
        } catch (NacosException e) {
            logger.error(e.getErrMsg(),e);
        }
        return false;
    }

    @Override
    public String getPath(String key) {
        return wrapper(key);
    }

    @Override
    public String getPath(String group, String key) {
        throw new RuntimeException("not support!");
    }

    private ConfigService buildConfigService(URL url) {
        Properties properties = buildNacosProperties(url);
        try {
            configService = NacosFactory.createConfigService(properties);
        } catch (NacosException e) {
            logger.error(e.getErrMsg(), e);
            throw new IllegalStateException(e);
        }
        return configService;
    }

    private Properties buildNacosProperties(URL url) {
        Properties properties = new Properties();
        setServerAddr(url, properties);
        return properties;
    }

    private void setServerAddr(URL url, Properties properties) {

        String serverAddr = url.getHost() + // Host
                ":" +
                url.getPort() // Port
                ;
        properties.put(SERVER_ADDR, serverAddr);
    }

    private String wrapper(String key) {
        Matcher matcher = pattern.matcher(key);
        if(matcher.find()){
            return matcher.group(1);
        }
        return key;
    }


}
