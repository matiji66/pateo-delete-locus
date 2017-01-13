package com.pateo.hbase.deletelocus;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pateo.constant.Constant;
import com.sparkproject.conf.ConfigurationManager;

/**
 * @author sh04595
 */
public class DeleteAbortedUserHbaseData {

	static Logger LOG = LoggerFactory.getLogger(DeleteAbortedUserHbaseData.class);
	static HBaseAdmin admin = null;
	private static Table table = null;
	private static Connection connection = null;
	public static void main(String[] args) {

		
		initTable();
		LOG.info(" Table connected end");

		// 17 位 P006000000000000
		String obd_id = "";
		
		inviladLocus(obd_id);
		
		LOG.info(" inviladAllObdLocus end " );
		closeTable();
		LOG.info(" ----- Finished!  ----- ");
	}

	private static void inviladLocus(String obd_id) {
		List<String> keys = scanAllObdLocus(obd_id);
		inviladAllObdLocus(keys);
	}

	private static void initTable() {
		long start1 = System.currentTimeMillis();
		LOG.info("  init connection ----- " + start1);
		
		Configuration configuration = HBaseConfiguration.create();
		configuration.set(Constant.hbase_zookeeper_quorum, ConfigurationManager
				.getProperty(Constant.prod_hbase_zookeeper_quorum));
		// configuration.set("hbase.zookeeper.property.clientPort",
		// ConfigurationManager.getProperty(Constant.hbase_zookeeper_clientPort));
		configuration.set("hbase.zookeeper.property.clientPort",
				ConfigurationManager
						.getProperty(Constant.prod_hbase_zookeeper_clientPort));

		try {
			connection = ConnectionFactory.createConnection(configuration);
		} catch (IOException e2) {
			e2.printStackTrace();
		}
		 
		String tableName = "obd_locus:locus_travel";

		try {
			table = connection.getTable(TableName.valueOf(tableName));
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	/**
	 * 关闭连接
	 */
	private static void closeTable() {
		try {
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		try {
			connection.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static List<String> scanAllObdLocus(String obd_id) {

		Scan scan = new Scan();
		long startseconds = dateConvertToMills("2010-01-01 00:00:00","yyyy-MM-dd HH:mm:ss") / 1000;
		long endseconds = System.currentTimeMillis() / 1000;
		LOG.info(" -setStartRow :" + (obd_id + "_" + startseconds));
		LOG.info(" -setStopRow  :" + (obd_id + "_" + endseconds));

		scan.setStartRow(Bytes.toBytes(obd_id + "_" + startseconds));
		scan.setStopRow(Bytes.toBytes(obd_id + "_" + endseconds));
		// scan.setReversed(true);

		ResultScanner rs = null;
		Integer count = 0;
		List<String> keys = new ArrayList<String>();
		try {
			rs = table.getScanner(scan);
			for (Result r : rs) {
				Cell[] rawCells = r.rawCells();
				Cell cell1 = rawCells[0];
				byte[] cloneRow1 = CellUtil.cloneRow(cell1);
				String key = Bytes.toString(cloneRow1);
				keys.add(key);
				count++;
				LOG.info("key" + count + ":" + key);
			}
			LOG.info("  -----" + count + " row");
		} catch (IOException e) {
			e.printStackTrace();
		}

		return keys;
	}

	/**
	 * 扫描出指定时间范围内的轨迹
	 * @param startTime yyyy-MM-dd HH:mm:ss
	 * @param endTime  yyyy-MM-dd HH:mm:ss
	 * @param obd_id
	 * @param table hbase table
	 * @return 返回所有要置为无效的轨迹 keys
	 */
	public static List<String> scanObdLocus(String startTime, String endTime,String obd_id) {
		Scan scan = new Scan();
		long startseconds = dateConvertToMills(startTime,"yyyy-MM-dd HH:mm:ss") / 1000;
		long endseconds = (dateConvertToMills(endTime, "yyyy-MM-dd HH:mm:ss") / 1000);
		LOG.info(" -setStartRow :" + (obd_id + "_" + startseconds));
		LOG.info(" -setStopRow  :" + (obd_id + "_" + endseconds));

		scan.setStartRow(Bytes.toBytes(obd_id + "_" + startseconds));
		scan.setStopRow(Bytes.toBytes(obd_id + "_" + endseconds));
		// scan.setReversed(true);

		ResultScanner rs = null;
		Integer count = 0;
		List<String> keys = new ArrayList<String>();
		try {
			rs = table.getScanner(scan);
			for (Result r : rs) {
				Cell[] rawCells = r.rawCells();
				Cell cell1 = rawCells[0];
				byte[] cloneRow1 = CellUtil.cloneRow(cell1);
				String key = Bytes.toString(cloneRow1);
				keys.add(key);
				count++;
				LOG.info("key" + count + ":" + key);
			}
			LOG.info("----------------------" + count + " row ");
		} catch (IOException e) {
			e.printStackTrace();
		}
		return keys;
	}
 
	/**
	 * 轨迹invalid字段置为0即为无效
	 * @param keys 无效的keys
	 */
	private static void inviladAllObdLocus(List<String> keys) {
		for (String rowkey : keys) {
			LOG.info("rowkey:" + rowkey);
			Put put = new Put(Bytes.toBytes(rowkey));
			byte[] family = Bytes.toBytes("f1");
			// isvalid
			byte[] qualify_c1 = Bytes.toBytes("isvalid");
			byte[] value_on = Bytes.toBytes("0");
			put.addColumn(family, qualify_c1, value_on);
			try {
				table.put(put);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	// 时间转换工具
	private static long dateConvertToMills(String date, String format) {
		Calendar c = Calendar.getInstance();
		try {
			c.setTime(new SimpleDateFormat(format).parse(date));
		} catch (java.text.ParseException e) {
			e.printStackTrace();
		}
		return c.getTimeInMillis();
	}

}