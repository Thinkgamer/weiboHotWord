package org.apache.mr.hotword;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by thinkgamer on 16-9-22.
 */
public class inHive {

    private static String Driver ="org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://127.0.0.1:10000/weibohotword";
    private static String name = "root";
    private static String password = "root";

    public static void run(Map<String, String> path) {
        try {
            Class.forName(Driver);
            Connection conn = DriverManager.getConnection(url, name, password);
            Statement stat = conn.createStatement();
            String sql = "load data inpath '/file/weibo/output/20160221-r-00000' into table hot_word partition(timeday='20160221')" ;
            System.out.println(sql);
            stat.execute(sql);
            /*
            String sql = "show databases";
            ResultSet rs = stat.executeQuery(sql);
            while (rs.next()) {
                System.out.println(rs.getString(1));//hive的索引从1开始
            }
            */
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public static Map<String, String> getResult() {
        Map<String, String> map = new TreeMap<String, String>();
        try {
            Class.forName(Driver);
            Connection conn = DriverManager.getConnection(url, name, password);
            Statement stat = conn.createStatement();
            String sql = "select * from hot_word where timeday='20160221'";
            ResultSet rs = stat.executeQuery(sql);
            while (rs.next()) {
//                System.out.println(rs.getString(2) +"\t"+rs.getString(4));//hive的索引从1开始
                map.put(rs.getString(2),rs.getString(4));
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return map;
    }

}
