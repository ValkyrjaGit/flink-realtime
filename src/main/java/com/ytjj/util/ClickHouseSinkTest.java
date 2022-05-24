package com.ytjj.util;

import com.ytjj.common.GmallConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHouseSinkTest {
    public static void main(String[] args) {
        Connection connection = null;
        try {
            Class.forName(GmallConfig.CLICKHOUSE_DRIVER);
            connection = DriverManager.getConnection(GmallConfig.CLICKHOUSE_URL);
        } catch (Exception e) {
            e.printStackTrace();
        }
        String sql = "insert into rmt_table values('2022-05-24',2,'b',50)";
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
