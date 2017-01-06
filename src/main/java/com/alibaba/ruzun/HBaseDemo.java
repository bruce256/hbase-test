package com.alibaba.ruzun;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;


public class HBaseDemo {

	private static Configuration conf       = null;
	public static  String        tableName  = "table1";
	private static TableName     TABLE_NAME = TableName.valueOf(Bytes.toBytes(tableName));
	private static Connection    connection = null;
	private static Table         table      = null;

	/**
	 * 初始化配置
	 */
	static void init() {
		conf = HBaseConfiguration.create();
		try {
			connection = ConnectionFactory.createConnection(conf);
			table = connection.getTable(TABLE_NAME);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 查询表中所有行
	 */
	public void scan() {
		System.out.println("scan");
		try {
			Scan          s  = new Scan();
			ResultScanner rs = table.getScanner(s);
			for (Result r : rs) {
				List<Cell> cells = r.listCells();
				cells.forEach(cell -> printCell(cell));
					/*System.out.print(Bytes.toString(copyOfRange(kv[i].getRowArray(), kv[i].getRowOffset(),
							kv[i].getRowOffset() + kv[i].getRowLength())));
					System.out.print(Bytes.toString(copyOfRange(kv[i].getFamilyArray(), kv[i].getFamilyOffset(),
							kv[i].getFamilyOffset() + kv[i].getFamilyLength())));
					System.out.print(Bytes.toString(copyOfRange(kv[i].getQualifierArray(), kv[i].getQualifierOffset(),
							kv[i].getQualifierOffset() + kv[i].getQualifierLength())));
					System.out.print(kv[i].getTimestamp());
					System.out.println(Bytes.toString(copyOfRange(kv[i].getValueArray(), kv[i].getValueOffset(),
							kv[i].getValueOffset() + kv[i].getValueLength())));*/
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void scanByRange() {
		System.out.println("scanByRange");
		try {
			Scan s = new Scan();
			s.setStartRow(Bytes.toBytes("row"));
			s.setStopRow(Bytes.toBytes("row9"));
			ResultScanner rs = table.getScanner(s);
			for (Result r : rs) {
				List<Cell> cells = r.listCells();
				cells.forEach(cell -> printCell(cell));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 插入一行记录
	 */
	public void put(String row, String family, String qualifier, String value) {
		System.out.println("put");
		try {
			Put put = new Put(Bytes.toBytes(row));
			put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void get(String row) {
		System.out.println("get");
		Get get = new Get(Bytes.toBytes(row));
		get.addFamily(Bytes.toBytes("col1"));
		try {
			Result     result = table.get(get);
			List<Cell> cells  = result.listCells();
			cells.forEach(cell -> printCell(cell));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void getFamily(String row) {
		System.out.println("getFamily");
		Get get = new Get(Bytes.toBytes(row));
		get.addFamily(Bytes.toBytes("col1"));
		try {
			Result     result = table.get(get);
			List<Cell> cells  = result.listCells();
			cells.forEach(cell -> printCell(cell));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void delete(String row) {
		System.out.println("delete");
		Delete delete = new Delete(Bytes.toBytes(row));
		try {
			table.delete(delete);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void printCell(Cell cell) {
		System.out.println(new String(cell.getRow()) + '\t'
								   + new String(cell.getFamily()) + '\t'
								   + new String(cell.getQualifier()) + '\t'
								   + new String(cell.getValue()));
	}

	public void addColumnFamily(String familyName) {
		try {
			HBaseAdmin       admin      = new HBaseAdmin(conf);
			HTableDescriptor descriptor = new HTableDescriptor(table.getTableDescriptor());
			descriptor.addFamily(new HColumnDescriptor(familyName));
			admin.disableTable(TABLE_NAME);
			admin.modifyTable(TABLE_NAME, descriptor);
			admin.enableTable(TABLE_NAME);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		init();
		HBaseDemo hBaseDemo = new HBaseDemo();
		/*hBaseDemo.addColumnFamily("col2");*/
		for (int i = 0; i < 20; i++) {
			hBaseDemo.put("row" + i, "col3", "z", "ruzun");
		}
		hBaseDemo.get("row");
		hBaseDemo.getFamily("row");
		hBaseDemo.scan();
		/*hBaseDemo.delete("rows1");*/
		hBaseDemo.scan();
		hBaseDemo.scanByRange();
	}
}
