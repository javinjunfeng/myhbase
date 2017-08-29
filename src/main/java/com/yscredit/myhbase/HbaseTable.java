package com.yscredit.myhbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * Created by katarina on 2017/8/28.
 */
public class HbaseTable {
    public static void createTables(Configuration config) {
        Connection connection = null;
        Admin admin = null;
        try {
            connection = ConnectionFactory.createConnection(config);
            admin = connection.getAdmin();
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf("mytable"));    //表名
            table.addFamily(new HColumnDescriptor("info")); //列簇名
            if (admin.tableExists(table.getTableName())) {  //如果表已存在，先禁用，再删除
                admin.disableTable(table.getTableName());
                admin.deleteTable(table.getTableName());
            }
            admin.createTable(table);   //创建表
            System.out.println("表创建成功！");
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                admin.close();
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void modifyTable(Configuration config) {
        Connection connection = null;
        Admin admin = null;
        try {
            connection = ConnectionFactory.createConnection(config);
            admin = connection.getAdmin();
            TableName tableName = TableName.valueOf("mytable"); //表名
            if (!admin.tableExists(tableName)) {
                System.out.println("表不存在！");
                System.exit(-1);
            }

            HTableDescriptor table = admin.getTableDescriptor(tableName);

            //添加列簇
            HColumnDescriptor newColumn = new HColumnDescriptor("info1");
			//admin.addColumn(tableName, newColumn);

            //删除列簇
			//admin.deleteColumn(tableName, "mycf01".getBytes("UTF-8"));

            //删除表
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                admin.close();
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    public static void main(String[] args) {
        Configuration config = HBaseConfiguration.create();
		createTables(config);
        //modifyTable(config);
    }
}
