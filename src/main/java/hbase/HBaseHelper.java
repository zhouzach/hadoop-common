package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class HBaseHelper {

    private static Connection connection;

    static {
        Configuration configuration = HBaseConfiguration.create();
//            configuration.set("hbase.zookeeper.property.clientPort", "2181");
        // 如果是集群 则主机名用逗号分隔
//            configuration.set("hbase.zookeeper.quorum", "hadoop001");
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
//            System.out.println(connection);
//        createTable("users", Arrays.asList("info"));

//        putRow("users","-123tom","info", "name", "tom");

//        List<Pair<String, String>> infoPairs = Arrays.asList(
//                Pair.newPair("age", "20"),
//                Pair.newPair("score", "95")
//        );
//        putRow("users", "-123jack", "info", infoPairs);
//        List<Pair<String, String>> codingPairs = Arrays.asList(
//                Pair.newPair("scala", "1"),
//                Pair.newPair("java", "1")
//        );
//        putRow("users", "-123tom", "coding", codingPairs);

//        List<Pair<String, String>> infoPairs = Arrays.asList(
//                Pair.newPair("age", "18"),
//                Pair.newPair("score", "80")
//        );
//        putRow("users", "-124lily", "info", infoPairs);

//        addColumnFamily("users", Arrays.asList("coding2"));
//        deleteColumnFamily("users", Arrays.asList("coding1"));
//        deleteRow("users","-123tom");

//        ResultScanner rs = getScanner("users");
        ResultScanner rs = getScanner("users","-123","-124",null);
        listResult(rs);
//        System.out.println();
//        listResultByRowKey("users","-123tom");
//        listTableNames();

//        deleteTable("users");

        close(connection);
    }

    /**
     * 创建 HBase 表
     *
     * @param tableName      表名
     * @param columnFamilies 列族的数组
     */
    public static boolean createTable(String tableName, List<String> columnFamilies) {
        HBaseAdmin admin = null;
        try {
            admin = (HBaseAdmin) connection.getAdmin();
            if (admin.tableExists(TableName.valueOf(tableName))) {
                return false;
            }
            TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
            columnFamilies.forEach(columnFamily -> {
                ColumnFamilyDescriptorBuilder cfDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));
                cfDescriptorBuilder.setMaxVersions(1);
                ColumnFamilyDescriptor familyDescriptor = cfDescriptorBuilder.build();
                tableDescriptor.setColumnFamily(familyDescriptor);
            });
            admin.createTable(tableDescriptor.build());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return true;
    }


    /**
     * 删除 hBase 表
     *
     * @param tableName 表名
     */
    public static boolean deleteTable(String tableName) {
        HBaseAdmin admin = null;
        try {
            admin = (HBaseAdmin) connection.getAdmin();
            // 删除表前需要先禁用表
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println("deleted table: "+ tableName);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("deleted table: "+ tableName + "failed!");
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return true;
    }

    /**
     * 插入数据
     *
     * @param tableName        表名
     * @param rowKey           唯一标识
     * @param columnFamilyName 列族名
     * @param qualifier        列标识
     * @param value            数据
     */
    public static boolean putRow(String tableName, String rowKey, String columnFamilyName, String qualifier,
                                 String value) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(put);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }


    /**
     * 插入数据
     *
     * @param tableName        表名
     * @param rowKey           唯一标识
     * @param columnFamilyName 列族名
     * @param pairList         列标识和值的集合
     */
    public static boolean putRow(String tableName, String rowKey, String columnFamilyName, List<Pair<String, String>> pairList) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            pairList.forEach(pair -> put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(pair.getFirst()), Bytes.toBytes(pair.getSecond())));
            table.put(put);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }


    /**
     * 根据 rowKey 获取指定行的数据
     *
     * @param tableName 表名
     * @param rowKey    唯一标识
     */
    public static Result getRow(String tableName, String rowKey) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            return table.get(get);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     * 获取指定行指定列 (cell) 的最新版本的数据
     *
     * @param tableName    表名
     * @param rowKey       唯一标识
     * @param columnFamily 列族
     * @param qualifier    列标识
     */
    public static String getCell(String tableName, String rowKey, String columnFamily, String qualifier) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            if (!get.isCheckExistenceOnly()) {
                get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
                Result result = table.get(get);
                byte[] resultValue = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
                return Bytes.toString(resultValue);
            } else {
                return null;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 删除指定行记录
     *
     * @param tableName 表名
     * @param rowKey    唯一标识
     */
    public static boolean deleteRow(String tableName, String rowKey) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }


    /**
     * 删除指定行指定列
     *
     * @param tableName  表名
     * @param rowKey     唯一标识
     * @param familyName 列族
     * @param qualifier  列标识
     */
    public static boolean deleteColumn(String tableName, String rowKey, String familyName,
                                       String qualifier) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            delete.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(qualifier));
            table.delete(delete);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 检索全表
     *
     * @param tableName 表名
     */
    public static ResultScanner getScanner(String tableName) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            return table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     * 检索表中指定数据
     *
     * @param tableName  表名
     * @param filterList 过滤器
     */

    public static ResultScanner getScanner(String tableName, FilterList filterList) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.setFilter(filterList);
            return table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 检索表中指定数据
     *
     * @param tableName   表名
     * @param startRowKey 起始 RowKey
     * @param endRowKey   终止 RowKey
     * @param filterList  过滤器
     */

    public static ResultScanner getScanner(String tableName, String startRowKey, String endRowKey,
                                           FilterList filterList) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.withStartRow(Bytes.toBytes(startRowKey));
            scan.withStopRow(Bytes.toBytes(endRowKey));
            scan.setFilter(filterList);
            return table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void getResult(ResultScanner resultScanner) {
        for (Result r : resultScanner) {
            byte[] b = r.getValue(Bytes.toBytes("cf"), Bytes.toBytes("name"));
            String name = Bytes.toString(b);

            b = Arrays.copyOfRange(r.getRow(), 10, 15);
            DateTime dateTime = new DateTime(-1 * Bytes.toLong(b));
        }

    }

    public static void listResult(ResultScanner resultScanner) {
        for (Result r : resultScanner) {
            String rowKey = Bytes.toString(r.getRow());
            System.out.println("rowKey: " + rowKey);
            r.getMap().forEach((family, map) -> {
                map.forEach((column, kv) -> {
                    kv.forEach((t, v) -> {
                        System.out.println("family: " + Bytes.toString(family) +
                                ", column: " + Bytes.toString(column) +
                                ", t: " + t +
                                ", v: " + Bytes.toString(v) + ";");
                    });

                });
            });


        }

    }

    //https://www.programcreek.com/java-api-examples/index.php?api=org.apache.hadoop.hbase.Cell
    public static void listResultByRowKey(String tableName, String rowKey) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));

            Result result = table.get(get);

            final CellScanner cellScanner = result.cellScanner();
            while (cellScanner.advance()){
                final Cell cell = cellScanner.current();

                // get the column family
                final byte[] colunmnFamily = new byte[cell.getFamilyLength()];
                System.arraycopy(cell.getFamilyArray(), cell.getFamilyOffset(),
                        colunmnFamily, 0, cell.getFamilyLength());
                System.out.print(Bytes.toString(colunmnFamily) + ",");

                // get the column qualifier
                final byte[] colunmnQualifier = new byte[cell.getQualifierLength()];
                System.arraycopy(cell.getQualifierArray(), cell.getQualifierOffset(),
                        colunmnQualifier, 0, cell.getQualifierLength());
                System.out.print(Bytes.toString(colunmnQualifier)+ ",");

                // get the value
                final byte[] value = new byte[cell.getValueLength()];
                System.arraycopy(cell.getValueArray(), cell.getValueOffset(),
                        value, 0, cell.getValueLength());
                System.out.println(Bytes.toString(value));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void addColumnFamily(String sTalbeName, List<String> columnFamilies) {
        Admin admin = null;
        try {
            Configuration config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum", "localhost");
            connection = ConnectionFactory.createConnection(config);
            admin = connection.getAdmin();

            TableName tableName = TableName.valueOf(sTalbeName);

            // 判断是否存在此表
            boolean ifexists = admin.tableExists(tableName);

            if (ifexists) {

                if (!admin.isTableDisabled(tableName)) {
                    // 如果没关闭则关闭
                    admin.disableTable(tableName);
                    System.out.println(tableName + " disable...");
                }

                for (int i = 0; i < columnFamilies.size(); i++) {
                    String columnFamily = columnFamilies.get(i);
                    ColumnFamilyDescriptorBuilder cfDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));
                    cfDescriptorBuilder.setMaxVersions(1);
                    ColumnFamilyDescriptor familyDescriptor = cfDescriptorBuilder.build();
                    admin.addColumnFamily(tableName, familyDescriptor);
                }

                if (!admin.isTableEnabled(tableName)) {
                    // 如果没有打开则打开表
                    admin.enableTable(tableName);
                    System.out.println(tableName + " enable...");
                }

                System.out.println(sTalbeName
                        + " add And modify column success!");

            } else {
                System.out.println("There is not " + sTalbeName);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void deleteColumnFamily(String sTalbeName, List<String> columnFamilies) {
        Admin admin = null;
        try {
            Configuration config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum", "localhost");
            connection = ConnectionFactory.createConnection(config);
            admin = connection.getAdmin();

            TableName tableName = TableName.valueOf(sTalbeName);

            // 判断是否存在此表
            boolean ifexists = admin.tableExists(tableName);

            if (ifexists) {

                if (!admin.isTableDisabled(tableName)) {
                    // 如果没关闭则关闭
                    admin.disableTable(tableName);
                    System.out.println(tableName + " disable...");
                }

                for (int i = 0; i < columnFamilies.size(); i++) {
                    String columnFamily = columnFamilies.get(i);
                    admin.deleteColumnFamily(tableName, Bytes.toBytes(columnFamily));
                }

                if (!admin.isTableEnabled(tableName)) {
                    // 如果没有打开则打开表
                    admin.enableTable(tableName);
                    System.out.println(tableName + " enable...");
                }

                System.out.println(sTalbeName
                        + " delete column success!");

            } else {
                System.out.println("There is not " + sTalbeName);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void listTableNames() {
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            System.out.println(Arrays.toString(admin.listTableNames()));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void close(Connection connection) {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
