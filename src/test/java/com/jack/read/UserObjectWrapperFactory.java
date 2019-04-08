package com.jack.read;

import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.wrapper.ObjectWrapper;
import org.apache.ibatis.reflection.wrapper.ObjectWrapperFactory;

/**
 * @description: 自定义 UserObjectWrapperFactory
 * @author: JackWu
 * @create: 2019-04-06 09:57
 **/
public class UserObjectWrapperFactory implements ObjectWrapperFactory{

    @Override
    public boolean hasWrapperFor(Object object) {
        return false;
    }

    @Override
    public ObjectWrapper getWrapperFor(MetaObject metaObject, Object object) {
        return null;
    }
}
