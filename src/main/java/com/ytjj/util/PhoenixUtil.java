package com.ytjj.util;

import com.ytjj.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PhoenixUtil {
    private static Connection connection;

    private static Connection init() {
        try {
            Class.forName(GmallConfig.PHOENIX_DRIVER);
            DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
            connection.setSchema(GmallConfig.HBASE_SCHEMA);
            return connection;
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("获取phoenix连接失败！");
        }
    }

    public static <T> List<T> queryList(String sql, Class<T> cls) {
        //初始化连接
        if (connection == null) {
            connection = init();
        }
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            //编译 SQL
            preparedStatement = connection.prepareStatement(sql);
            //执行查询
            resultSet = preparedStatement.executeQuery();
            //获取查询结果中的元数据信息
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            ArrayList<T> list = new ArrayList<>();
            while (resultSet.next()) {
                T t = cls.newInstance();
                for (int i = 1; i < columnCount + 1; i++) {
                    BeanUtils.setProperty(t, metaData.getColumnName(i),
                            resultSet.getObject(i));
                }
                list.add(t);
            }
            //返回结果
            return list;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("查询维度信息失败！");
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {

    }
}
