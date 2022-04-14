package com.tencent.mdfs.provider.util;

import java.io.IOException;
import java.io.Reader;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

/**
 * create:chunxiaoli
 * Date:4/8/18
 */
public class BDMSMyBatisUtil {
    private final static SqlSessionFactory sqlSessionFactory;
    private final static SqlSession sqlSession;

    private static ThreadLocal<SqlSession> threadLcoal = new ThreadLocal<SqlSession>();

    static {
        String resource = "mybatis2.xml";
        Reader reader = null;
        try {
            reader = Resources.getResourceAsReader(resource);
        } catch (IOException e) {
            System.out.println(e.getMessage());

        }
        sqlSessionFactory = new SqlSessionFactoryBuilder().build(reader);
        sqlSession=sqlSessionFactory.openSession(true);
        //Connection connection= sqlSession.getConnection();
        try {
            System.out.println("@@@@@@@@@@@@@@@MyBatisUtil init ok@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    public static SqlSessionFactory getSqlSessionFactory() {
        return sqlSessionFactory;
    }

    public synchronized static SqlSession open(){
        return sqlSessionFactory.openSession(true);
    }

    public synchronized static <T>  T getMapper(Class<T> clz){
        return getSqlSession().getMapper(clz);
    }

    public static void clearCache(){
        sqlSession.clearCache();
    }


    public synchronized static  SqlSession getSqlSession(){
        SqlSession sqlSession = threadLcoal.get();
        if(sqlSession == null){
            sqlSession = open();
            threadLcoal.set(sqlSession);
        }
        return sqlSession;
    }

    public synchronized static void closeSqlSession(){
        SqlSession sqlSession = threadLcoal.get();
        if(sqlSession != null){
            sqlSession.close();
            threadLcoal.remove();
        }
    }
}
