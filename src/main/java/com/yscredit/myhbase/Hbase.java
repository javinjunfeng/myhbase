package com.yscredit.myhbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by katarina on 2017/8/28.
 */

/**
 * 对Hbase表数据进行增、删、改、查操作
 */
public class Hbase {
    public static HTable getTable(String tableName) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, tableName);
        return table;
    }

    /**
     * 查询单条记录数据
     * @param rowkey
     * @throws IOException
     */
    public static void getData(String rowkey) throws IOException {
        HTable table = Hbase.getTable("mytable");

        Get get = new Get(Bytes.toBytes(rowkey));
        Result result = table.get(get);
        Cell[] cells = result.rawCells();

        for (Cell cell : cells) {
            System.out.print(Bytes.toString(CellUtil.cloneRow(cell)) + "\t");
            System.out.print(Bytes.toString(CellUtil.cloneFamily(cell)) + ":");
            System.out.print(Bytes.toString(CellUtil.cloneQualifier(cell)) +"\t" );
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
        }
        table.close();
    }

    /**
     * 获取所有数据
     * @throws IOException
     */
    public static void scanTable() throws IOException {
        HTable table = Hbase.getTable("mytable");
        Scan scan = new Scan();
//		scan.setStartRow(Bytes.toBytes("10002"));
//		scan.setStopRow(Bytes.toBytes("10004"));
        ResultScanner resultScanner = table.getScanner(scan);

        for (Result row : resultScanner) {
            System.out.println("row:");
            Cell[] cells = row.rawCells();
            for (Cell cell : cells) {
                System.out.print(Bytes.toString(CellUtil.cloneRow(cell)) + "\t");
                System.out.print(Bytes.toString(CellUtil.cloneFamily(cell)) + ":");
                System.out.print(Bytes.toString(CellUtil.cloneQualifier(cell)) + "==>");
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

    /**
     * 添加数据
     * @throws IOException
     */
    public static void putData() throws IOException {
        HTable table = Hbase.getTable("mytable");
        Put put = new Put(Bytes.toBytes("1001"));
        put.add(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes("ziyu")); //列簇：info 、字段：name、值：ziyu
        put.add(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes("21"));
        put.add(Bytes.toBytes("info"),Bytes.toBytes("sex"),Bytes.toBytes("male"));

        table.put(put);
        table.close();
    }

    /**
     * 删除数据
     * @throws IOException
     */
    private static void deleteData() throws IOException{
        HTable table = Hbase.getTable("mytable");
        Delete delete = new Delete(Bytes.toBytes("1002"));
        delete.deleteColumns(Bytes.toBytes("info"),Bytes.toBytes("age"));
        table.delete(delete);
        table.close();

    }

    public static void main(String[] args) throws IOException {
//        Hbase.getData("10001");   //查询key为“10001”的数据
        Hbase.scanTable();        //扫描表所有信息
//        Hbase.putData();          //添加数据
//        Hbase.scanTable();
//        Hbase.deleteData();       //删除数据
//        Hbase.scanTable();
    }
}
