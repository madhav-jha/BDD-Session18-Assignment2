package session18.assignment2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class CompanyTable {

	static void createTable() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);
		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("company"));
		tableDescriptor.addFamily(new HColumnDescriptor("details"));
		System.out.println("Creating HBase Table...");
		admin.createTable(tableDescriptor);
		System.out.println("HBase Table created...");

	}

	static void insertRecord() throws Exception {

		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, "company");

		Put put = new Put(Bytes.toBytes("Microsoft"));
		put.add(Bytes.toBytes("details"), Bytes.toBytes("CEO"), Bytes.toBytes("Satya Nadela"));
		put.add(Bytes.toBytes("details"), Bytes.toBytes("Employees"), Bytes.toBytes("30000"));
		table.put(put);

		put = new Put(Bytes.toBytes("Oracle"));
		put.add(Bytes.toBytes("details"), Bytes.toBytes("CEO"), Bytes.toBytes("Larry Ellison"));
		put.add(Bytes.toBytes("details"), Bytes.toBytes("Employees"), Bytes.toBytes("40000"));
		table.put(put);

		put = new Put(Bytes.toBytes("Google"));
		put.add(Bytes.toBytes("details"), Bytes.toBytes("CEO"), Bytes.toBytes("Sunder Pichai"));
		put.add(Bytes.toBytes("details"), Bytes.toBytes("Employees"), Bytes.toBytes("60000"));
		table.put(put);

		table.close();
	}

	static void readRecord() throws Exception {

		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, "company");
		Get get = new Get(Bytes.toBytes("Microsoft"));
		Result rs = table.get(get);

		for (KeyValue kv : rs.raw()) {
			System.out.print(new String(kv.getRow()) + " ");
			System.out.print(new String(kv.getFamily()) + ":");
			System.out.print(new String(kv.getQualifier()) + " ");
			System.out.print(kv.getTimestamp() + " ");
			System.out.println(new String(kv.getValue()));
		}
	}

	static void display() throws Exception {

		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, "company");
		Scan sc = new Scan();
		ResultScanner rs = table.getScanner(sc);
		System.out.println("Get all records\n");
		for (Result r : rs) {
			for (KeyValue kv : r.raw()) {
				System.out.print(new String(kv.getRow()) + " ");
				System.out.print(new String(kv.getFamily()) + ":");
				System.out.print(new String(kv.getQualifier()) + " ");
				System.out.print(kv.getTimestamp() + " ");
				System.out.println(new String(kv.getValue()));
			}
		}
	}

	public static void main(String[] args) throws Exception {

		createTable();
		insertRecord();
		readRecord();
		display();

	}

}
