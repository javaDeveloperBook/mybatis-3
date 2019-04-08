package com.jack.read;

import org.apache.ibatis.BaseDataTest;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

/**
 * @description: mybatis 框架源码阅读 demo 类
 * @author: JackWu
 * @create: 2019-04-05 14:49
 **/
public class MybatisReadTest {

    public static void main(String[] args) throws IOException, SQLException {
        // 获取 Mybatis 全局配置文件信息流
        InputStream inputStream = Resources.getResourceAsStream("com/jack/read/mybatis-config.xml");
        // 创建 SqlSessionFactory
        SqlSessionFactory sessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
        // 初始化 SQL 数据表
        BaseDataTest.runScript(sessionFactory.getConfiguration().getEnvironment().getDataSource(),"com/jack/read/CreateDB.sql");
        // 创建 SqlSession
        SqlSession sqlSession = sessionFactory.openSession();
        // 取得 UserMapper
        UserMapper userMapper = sqlSession.getMapper(UserMapper.class);
        // 查询结果遍历打印输出
        Cursor<User> users = userMapper.queryUsers();
        for (User user : users) {
            System.out.println(user);
        }
    }

}
