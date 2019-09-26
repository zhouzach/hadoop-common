import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.sql.*;

/**
 * https://blog.csdn.net/u013850277/article/details/77281229
 */
public class KerberosHiveHelper {
    /**
     * 用于连接Hive所需的一些参数设置 driverName:用于连接hive的JDBC驱动名 When connecting to
     * HiveServer2 with Kerberos authentication, the URL format is:
     * jdbc:hive2://<host>:<port>/<db>;principal=
     * <Server_Principal_of_HiveServer2>
     */
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://bigdata40:10000/admin;principal=hive/bigdata40@BIGDATA.COM";
    private static String sql = "";
    private static ResultSet res;

    public static Connection get_conn() throws SQLException, ClassNotFoundException {
        /** 使用Hadoop安全登录 **/
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");

        if (System.getProperty("os.name").toLowerCase().startsWith("win")) {
            // 默认：这里不设置的话，win默认会到 C盘下读取krb5.init
            System.setProperty("java.security.krb5.conf", "C:/Windows/krbconf/bms/krb5.ini");
        } // linux 会默认到 /etc/krb5.conf 中读取krb5.conf,本文笔者已将该文件放到/etc/目录下，因而这里便不用再设置了

        System.setProperty("java.security.krb5.conf", FilePathUtil.getPath("krb5.conf"));

        try {
            UserGroupInformation.setConfiguration(conf);
            //user: principal
            UserGroupInformation.loginUserFromKeytab("user@example.COM", FilePathUtil.getPath("user.keytab"));
        } catch (IOException e1) {
            e1.printStackTrace();
        }
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
    public static boolean show_tables(Statement statement) {
        sql = "SHOW TABLES";
//        sql = "SHOW DATABASES";
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
            System.out.println("connection: "+ conn);
            Statement stmt = conn.createStatement();
            // 创建的表名
            String tableName = "test_100m";
            show_tables(stmt);
            // describ_table(stmt, tableName);
            /** 删除表 **/
            // drop_table(stmt, tableName);
            // show_tables(stmt);
            // queryData(stmt, tableName);
//            createTable(stmt, tableName);
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("!!!!!!END!!!!!!!!");
        }
    }
}
