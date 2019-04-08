package com.jack.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * @description: 动态代理基础
 * @author: JackWu
 * @create: 2019-04-05 14:29
 **/
public class DynamicProxyTest {

    /**
     * 接口
     */
    interface Person {
        void sayHi(String name);
    }

    /**
     * 接口实现
     */
    static class PersonImpl implements Person {
        @Override
        public void sayHi(String name) {
            System.out.println("Hi " + name);
        }
    }

    static class PersonProxy implements InvocationHandler {
        private Person target;

        public PersonProxy(Person target) {
            this.target = target;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            // 执行目标方法之前的操作
            System.out.println("before ...");
            // 执行目标方法
            Object result = method.invoke(target, args);
            // 执行目标方法之后
            System.out.println("after ...");
            return result;
        }
    }


    public static void main(String[] args) {
        Person person = new PersonImpl();
        Person proxy = (Person) Proxy.newProxyInstance(person.getClass().getClassLoader(),
                person.getClass().getInterfaces(),new PersonProxy(person));
        proxy.sayHi("Jack");
    }

}
