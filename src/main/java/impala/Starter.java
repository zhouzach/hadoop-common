package impala;

import java.sql.ResultSet;

/**
 * Hello world!
 */
public class Starter {
    public static void main(String[] args) {

        String host = "";
        String user = "user@example.CN";
        String krb5confFilename = "";
        String keytabFilename = "";
        String sql = "";
        KerberosImpalaHelper jdbc = new KerberosImpalaHelper(host);
        Object obj = jdbc.user(user)
                .krb5Dest(krb5confFilename)
                .keytabDest(keytabFilename)
                .runWithKrbs(sql, new CallBack() {
                    public Object deal(Object obj) {
                        try {

                            if (obj instanceof ResultSet) {
                                ResultSet result = (ResultSet) obj;
                                StringBuilder builder = new StringBuilder();
                                while (result.next()) {
                                    builder.append(result.getString(1) + "\n");
                                }

                                return builder.toString();
                            }

                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                        return null;
                    }
                });

        System.out.println((obj != null && obj instanceof String) ? obj.toString() : "");

    }
}