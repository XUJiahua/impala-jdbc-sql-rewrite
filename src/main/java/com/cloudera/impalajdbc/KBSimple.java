package com.cloudera.impalajdbc;


import com.cloudera.utils.JDBCUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedAction;
import java.sql.*;


public class KBSimple {

    private static String JDBC_DRIVER = "com.cloudera.impala.jdbc.Driver";
    private static String CONNECTION_URL = "jdbc:impala://xh-hd2-peggy-dost000006:21050/product;AuthMech=1;KrbRealm=PEGGY.LING;KrbHostFQDN=xh-hd2-peggy-dost000006;KrbServiceName=impala";

    static {
        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static String readFile(String filename) throws IOException {
        File file = new File(filename);
        return FileUtils.readFileToString(file, StandardCharsets.UTF_8);
    }

    public static void main(String[] args) throws IOException {
        String filename = args[0];

        final String sql = readFile(filename);

        try {
            System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
            Configuration configuration = new Configuration();
            configuration.set("hadoop.security.authentication", "Kerberos");
            UserGroupInformation.setConfiguration(configuration);
            UserGroupInformation.loginUserFromKeytab("hive/xh-hd2-peggy-dost000003@PEGGY.LING", "/opt/metabase/hive.xh-hd2-peggy-dost000003.keytab");
            System.out.println(UserGroupInformation.getCurrentUser() + "------" + UserGroupInformation.getLoginUser());

            UserGroupInformation loginUser = UserGroupInformation.getLoginUser();

            loginUser.doAs((PrivilegedAction<Object>) () -> {
                Connection connection = null;
                ResultSet resultSet = null;
                PreparedStatement ps = null;
                try {
                    connection = DriverManager.getConnection(CONNECTION_URL);
                    ps = connection.prepareStatement(sql);
                    ps.setString(1, "jt20191017175015910985");
                    ps.setTimestamp(2, new Timestamp(2020 - 1900, 8 - 1, 1, 0, 0, 0, 0));
                    ps.setTimestamp(3, new Timestamp(2020 - 1900, 8 - 1, 5, 0, 0, 0, 0));
                    ps.setString(4, "jt20191017175015910985");
                    ps.setTimestamp(5, new Timestamp(2020 - 1900, 8 - 1, 1, 0, 0, 0, 0));
                    ps.setTimestamp(6, new Timestamp(2020 - 1900, 8 - 1, 5, 0, 0, 0, 0));
                    resultSet = ps.executeQuery();
                    ResultSetMetaData rsmd = resultSet.getMetaData();
                    int columnsNumber = rsmd.getColumnCount();
                    while (resultSet.next()) {
                        for (int i = 1; i <= columnsNumber; i++) {
                            if (i > 1) System.out.print(",  ");
                            String columnValue = resultSet.getString(i);
                            System.out.print(columnValue + " " + rsmd.getColumnName(i));
                        }
                        System.out.println("");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    JDBCUtils.disconnect(connection, resultSet, ps);
                }
                return null;
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
