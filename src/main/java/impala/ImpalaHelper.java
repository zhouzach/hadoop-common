package impala;

import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.sql.*;

public class ImpalaHelper {

    private static String driverName = "com.cloudera.impala.jdbc41.Driver";
    private static String url = ConnectUrl.url4Auth3;
    private static String sql = "";
    private static ResultSet res;

    public static Connection get_conn() throws SQLException, ClassNotFoundException {

        Class.forName(driverName);
        Connection conn = DriverManager.getConnection(url);
        return conn;
    }

    /**
     * 查看数据库下所有的表
     *
     * @param statement
     * @return
     */
    public static boolean show_databases(Statement statement) {
//        sql = "SHOW TABLES";
        sql = "SHOW DATABASES";
        System.out.println("Running:" + sql);
        try {
            ResultSet res = statement.executeQuery(sql);
            System.out.println("执行“+sql+运行结果:");
            while (res.next()) {
                System.out.println(res.getString(1));
            }
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 获取表的描述信息
     *
     * @param statement
     * @param tableName
     * @return
     */
    public static boolean describ_table(Statement statement, String tableName) {
        sql = "DESCRIBE " + tableName;
        try {
            res = statement.executeQuery(sql);
            System.out.print(tableName + "描述信息:");
            while (res.next()) {
                System.out.println(res.getString(1) + "\t" + res.getString(2));
            }
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 删除表
     *
     * @param statement
     * @param tableName
     * @return
     */
//    public static boolean drop_table(Statement statement, String tableName) {
//        sql = "DROP TABLE IF EXISTS " + tableName;
//        System.out.println("Running:" + sql);
//        try {
//            statement.execute(sql);
//            System.out.println(tableName + "删除成功");
//            return true;
//        } catch (SQLException e) {
//            System.out.println(tableName + "删除失败");
//            e.printStackTrace();
//        }
//        return false;
//    }

    /**
     * 查看表数据
     *
     * @param statement
     * @return
     */
    public static boolean queryData(Statement statement, String tableName) {
        sql = "SELECT * FROM " + tableName + " LIMIT 20";
        System.out.println("Running:" + sql);
        try {
            res = statement.executeQuery(sql);
            System.out.println("执行“+sql+运行结果:");
            while (res.next()) {
                System.out.println(res.getString(1) + "," + res.getString(2) + "," + res.getString(3));
            }
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 创建表
     *
     * @return
     */
//    public static boolean createTable(Statement statement, String tableName) {
//        sql = "CREATE TABLE test_1m_test2 AS SELECT * FROM test_1m_test"; //  为了方便直接复制另一张表数据来创建表
//        System.out.println("Running:" + sql);
//        try {
//            boolean execute = statement.execute(sql);
//            System.out.println("执行结果 ：" + execute);
//            return true;
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//        return false;
//    }

    public static void main(String[] args) {

        try {
            Connection conn = get_conn();

            System.out.println("con: "+conn);
            Statement stmt = conn.createStatement();
            show_databases(stmt);

//            String tableName = "infinivision_cdp.cdp_order_tag";
//
//             describ_table(stmt, tableName);

//             queryData(stmt, tableName);
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("!!!!!!END!!!!!!!!");
        }
    }
}
