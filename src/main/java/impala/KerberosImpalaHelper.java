package impala;

import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.sql.*;
import java.text.MessageFormat;

/**
 * https://www.jianshu.com/p/62aa4f9e0615
 * https://help.finebi.com/doc-view-293.html
 * https://cloud.tencent.com/developer/article/1078136
 * https://github.com/onefoursix/Cloudera-Impala-JDBC-Example
 */
public class KerberosImpalaHelper {
    private static String JDBC_DRIVER = "com.cloudera.impala.jdbc41.Driver";
    private static String CONNECTION_URL = "jdbc:impala://{0}:21050/;AuthMech=1;KrbRealm={1};KrbHostFQDN={0};KrbServiceName=impala";

    private static String SECURITY_KRB5_CONF = "java.security.krb5.conf";
    private static String HADOOP_SECURITY_AUTH = "hadoop.security.authentication";
    private static String DEFAULT_REALM = "EXAMPLE.CN";


    private String user;
    private String realm;
    private String krb5ConfDest = "krb5.conf";
    private String keytabDest;

    static {
        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public KerberosImpalaHelper(String deamonHost, String realm) {
        this.realm = realm;
        CONNECTION_URL = MessageFormat
                .format(CONNECTION_URL, deamonHost, realm);
    }

    public KerberosImpalaHelper(String deamonHost) {
        this(deamonHost, DEFAULT_REALM);
    }

    public KerberosImpalaHelper user(String user) {
        this.user = user;
        return self();
    }

    public KerberosImpalaHelper krb5Dest(String krb5ConfDest) {
        this.krb5ConfDest = krb5ConfDest;
        return self();
    }

    public KerberosImpalaHelper keytabDest(String keytabDest) {
        this.keytabDest = keytabDest;
        return self();
    }

    public Object runWithKrbs(final String sql,final CallBack func) {
        if (null == user || user.length() == 0) {
            throw new RuntimeException("用户不能为空!");
        }

        System.out.println("通过JDBC连接访问Kerberos环境下的Impala");
        // 登录Kerberos账号
        try {

            System.setProperty(SECURITY_KRB5_CONF,
                    FilePathUtil.getPath(krb5ConfDest));

//            org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
//            conf.set("hadoop.security.authentication", "Kerberos");
//            UserGroupInformation.setConfiguration(conf);

            UserGroupInformation.setConfiguration(KbsConfiguration
                    .newInstance().setPro(HADOOP_SECURITY_AUTH,
                            "Kerberos"));

            UserGroupInformation.loginUserFromKeytab(
                    user,
                    FilePathUtil.getPath(keytabDest == null?(user.replace(realm, "") + ".keytab"):keytabDest));

            UserGroupInformation logUser = UserGroupInformation.getLoginUser();

            if (null == logUser) {
                throw new RuntimeException("登录用户为空!");
            }
            System.out.println(UserGroupInformation.getCurrentUser() + "------"
                    + logUser );

            return logUser.doAs(new PrivilegedAction<Object>() {
                public Object run() {
                    Connection connection = null;
                    ResultSet rs = null;
                    PreparedStatement ps = null;
                    try {

                        //Class.forName(JDBC_DRIVER);
                        connection = DriverManager
                                .getConnection(CONNECTION_URL);
                        System.out.println("connection: " + connection);
                        ps = connection.prepareStatement(sql);
                        rs = ps.executeQuery();

                        if (null == func) {

                            return null;

                        } else {

                            return func.deal(rs);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            if (connection != null) {
                                connection.close();
                            }
                            if (ps != null) {
                                ps.close();
                            }
                            if (rs != null) {
                                rs.close();
                            }
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                    return null;
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
    private KerberosImpalaHelper self() {
        return this;
    }
}