package com.jack.read;

import org.apache.ibatis.reflection.factory.DefaultObjectFactory;

import java.util.Properties;

/**
 * @description: 自定义 ObjectFactory 类
 * @author: JackWu
 * @create: 2019-04-06 09:34
 **/
public class UserObjectFactory extends DefaultObjectFactory {
    private static final long serialVersionUID = 4576592418878031661L;
    private Properties properties;

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public Properties getProperties() {
        return properties;
    }

    @Override
    public void setProperties(Properties properties) {
        this.properties = properties;
    }
}
