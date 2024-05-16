package com.alibaba.ruzun;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;


@Slf4j
public class HBaseDemo {
	
	private Configuration conf       = null;
	public  String        tableName  = "user";
	private TableName     TABLE_NAME = TableName.valueOf(toBytes(tableName));
	private Connection    connection = null;
	private Table         table      = null;
	
	private int                coreSize = Runtime.getRuntime().availableProcessors();
	private ThreadPoolExecutor es       = new ThreadPoolExecutor(coreSize * 4, coreSize * 5, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>(1000000),
																 new ThreadPoolExecutor.CallerRunsPolicy());
	
	/**
	 * 初始化配置
	 */
	public void init() {
		conf = HBaseConfiguration.create();
		try {
			conf.set("hbase.zookeeper.quorum", "127.0.0.1");
			conf.set("hbase.zookeeper.property.clientPort", String.valueOf(2181));
			
			connection = ConnectionFactory.createConnection(conf);
			
			
			table = connection.getTable(TABLE_NAME);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void createTable() {
		Admin admin = null;
		try {
			admin = connection.getAdmin();
			//creating table descriptor
			HTableDescriptor t = new HTableDescriptor(TableName.valueOf(tableName));
			//creating column family descriptor
			HColumnDescriptor family = new HColumnDescriptor(toBytes("info"));
			t.addFamily(family);
			admin.createTable(t);
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
			s.setStartRow(toBytes("row999990"));
			s.setStopRow(toBytes("row999999"));
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
		log.info("put " + row);
		try {
			Put put = new Put(toBytes(row));
			put.addColumn(toBytes(family), toBytes(qualifier), toBytes(value));
			table.put(put);
		} catch (IOException e) {
			log.error(e.getMessage(), e);
		}
	}
	
	public void get(String row) {
		System.out.println("get");
		Get get = new Get(toBytes(row));
		get.addFamily(toBytes("col1"));
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
		Get get = new Get(toBytes(row));
		get.addFamily(toBytes("col1"));
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
		Delete delete = new Delete(toBytes(row));
		try {
			table.delete(delete);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void printCell(Cell cell) {
		System.out.println(new String(cell.getRowArray()) + '\t'
								   + new String(cell.getFamilyArray()) + '\t'
								   + new String(cell.getQualifierArray()) + '\t'
								   + cell.getTimestamp() + '\t'
								   + cell.getType() + '\t'
								   + new String(cell.getValueArray()));
	}
	
	public void addColumnFamily(String familyName) {
		/*try {
			HBaseAdmin       admin      = new HBaseAdmin(conf);
			HTableDescriptor descriptor = new HTableDescriptor(table.getTableDescriptor());
			descriptor.addFamily(new HColumnDescriptor(familyName));
			admin.disableTable(TABLE_NAME);
			admin.modifyTable(TABLE_NAME, descriptor);
			admin.enableTable(TABLE_NAME);
		} catch (IOException e) {
			e.printStackTrace();
		}*/
	}
	
	public void writeMassiveData(HBaseDemo hBaseDemo) {
		IntStream.range(0, 100000000).forEach(i -> {
			es.submit(() -> hBaseDemo.put("row" + i, "info", "name", "tangwanting" + i));
		});
		es.shutdown();
	}
	
	public static void main(String[] args) {
		HBaseDemo hBaseDemo = new HBaseDemo();
		hBaseDemo.init();
		/*hBaseDemo.createTable();*/
		hBaseDemo.scanByRange();
		/*hBaseDemo.addColumnFamily("col2");*/
/*		for (int i = 0; i < 1000000000; i++) {
		
		}*/
	/*	log.info("starting task!");
		long start = System.currentTimeMillis();
		hBaseDemo.writeMassiveData(hBaseDemo);
		log.info("数据写入结束, 耗时 " + (System.currentTimeMillis() - start) / 1000);*/
		/*hBaseDemo.get("row");
		hBaseDemo.getFamily("row");
		hBaseDemo.scan();
		*//*hBaseDemo.delete("rows1");*//*
		hBaseDemo.scan();
		hBaseDemo.scanByRange();*/
	}
}
