package impala;

/**
 * https://docs.cloudera.com/documentation/other/connectors/impala-jdbc/2-5-5/Cloudera-JDBC-Driver-for-Impala-Install-Guide-2-5-5.pdf
 *
 * Impala supports the following authentication mechanisms:
 *  No Authentication
 *  Kerberos
 *  User Name
 *  User Name and Password
 *  User Name and Password with Secure Sockets Layer
 */
public class ConnectUrl {

    /**
     * To use no authentication:
     *  Set the AuthMech property to 0
     */
    public static final String url4Auth0 = "jdbc:impala://localhost:21050;AuthMech=0";

    /**
     * Using Kerberos
     * For information on operating Kerberos, refer to the documentation for your operating system.
     * To configure the Cloudera JDBC Driver for Impala to use Kerberos authentication:
     * 1. Set the AuthMech property to 1.
     * 2. If your Kerberos setup does not define a default realm or if the realm of your Impala
     * server is not the default, then set the appropriate realm using the KrbRealm property.
     * 3. Set the KrbHostFQDN property to the fully qualified domain name of the Impala host.
     * 4. Set the KrbServiceName property to the service name of the Impala server.
     */
    public static final String url4Auth1 = "jdbc:impala://localhost:21050;AuthMech=1;KrbRealm=EXAMPLE.COM;KrbHostFQDN=impala.example.com;KrbServiceName=impala";

    /**
     * Using User Name
     * To configure User Name authentication:
     * 1. Set the AuthMech property to 2.
     * 2. Set the UID property to the appropriate user name recognized by the Impala server
     */
    public static final String url4Auth2 = "jdbc:impala://localhost:21050;AuthMech=2;UID=impala";

    /**
     * Using User Name and Password
     * To configure User Name and Password authentication:
     * 1. Set the AuthMech property to 3.
     * 2. Set the UID property to the appropriate user name recognized by the Impala server.
     * 3. Set the PWD property to the password corresponding to the user name you provided in
     * step 2.
     */
    public static final String url4Auth3 = "jdbc:impala://localhost:21050;AuthMech=3;UID=impala;PWD=*****";

    /**
     * Using User Name and Password with Secure Sockets Layer
     * To configure User Name and Password authentication using SSL:
     * 1. Create a KeyStore containing your signed, trusted SSL certificate.
     * 2. Set the AuthMech property to 4.
     * 3. Set the SSLKeyStore property to the full path of the KeyStore you created in step 1,
     * including the file name.
     * 4. Set the SSLKeyStorePwd property to the password for the KeyStore you created in step
     * 1.
     * 5. Set the UID property to the appropriate user name recognized by the Impala server.
     * 6. Set the PWD property to the password corresponding to the user name you provided in
     * step 5.
     */
    public static final String url4Auth4 = "jdbc:impala://localhost:21050;AuthMech=4;SSLKeyStore=C:\\Users\\bsmith\\Desktop\\keystore.jks;SSLKeyStorePwd=*****;UID=impala;PWD=*****";

}
