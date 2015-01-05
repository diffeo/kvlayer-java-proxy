package com.diffeo.kvlayer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ClientOnRequiredTable;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

/**
 * A basic performance test for Accumulo.
 * Similar to https://github.com/diffeo/kvlayer/blob/master/kvlayer/tests/performance.py
 */
public class PerfTestAccumulo implements Runnable {

	/**
	 * @param args
	 * @throws AccumuloSecurityException 
	 * @throws AccumuloException 
	 * @throws TableExistsException 
	 * @throws TableNotFoundException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException, IOException {
		testIntToBytesBE();
		
		FileWriter fout = new java.io.FileWriter("jaccumulo.tsv");
		PrintWriter out = new PrintWriter(fout);

		int[] itemSizes = new int[]{1024, 8192, 65536, 524288, 4194304};
		int[] numThreadOptions = new int[]{1,2,4,8};
		int[] itemsPerBatchOptions = new int[]{1,10,20,40};
		
		if (false) {
			// small test set
			itemSizes = new int[]{1024};
			numThreadOptions = new int[]{8};
			itemsPerBatchOptions = new int[]{40};
		}
		int numBatches = 50;
		
		PerfTestAccumulo tz = new PerfTestAccumulo("perftest", 0, 0, 0, 0);
		tz.ensureTable("perftest");

		out.println("#num_workers\titem_size\tnum_items_per_batch\tnum_batches\tX\tX\tX\tX\tinsert_bps\tget_bps");
		for (int numThreads : numThreadOptions) {
			for (int itemsPerBatch : itemsPerBatchOptions) {
				for (int itemSize : itemSizes) {
					session(out, numThreads, itemSize, itemsPerBatch, numBatches);
				}
			}
		}
		out.flush();
		fout.flush();
		fout.close();
		
		tz.connector.tableOperations().delete("perftest");
	}
	
	public static void session(PrintWriter out, int numThreads, int itemSize, int itemsPerBatch, int numBatches) {
		Thread[] th = new Thread[numThreads];
		PerfTestAccumulo[] they = new PerfTestAccumulo[numThreads];
		
		// WRITE
		for (int i = 0; i < numThreads; i++) {
			they[i] = new PerfTestAccumulo("perftest", itemSize, itemsPerBatch, numBatches, OP_WRITE);
			th[i] = new Thread(they[i]);
		}
		long start = System.currentTimeMillis();
		for (Thread thi : th) {
			thi.start();
		}
		for (Thread thi : th) {
			try {
				thi.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		long end = System.currentTimeMillis();
		long totalBytes = itemsPerBatch;
		totalBytes *= numBatches;
		totalBytes *= itemSize;
		totalBytes *= numThreads;
		double insertBps = totalBytes / ((end - start) / 1000.0);
		System.out.println(String.format("(%d*%d*%d*%d)=%d total bytes, %d ms, %f B/s", itemsPerBatch, numBatches, itemSize, numThreads, totalBytes, end - start, insertBps));
		
		// READ
		for (int i = 0; i < numThreads; i++) {
			they[i].op = OP_READ;
			th[i] = new Thread(they[i]);
		}
		start = System.currentTimeMillis();
		for (Thread thi : th) {
			thi.start();
		}
		for (Thread thi : th) {
			try {
				thi.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		end = System.currentTimeMillis();
		double getBps = totalBytes / ((end - start) / 1000.0);
		System.out.println(String.format("(%d*%d*%d*%d)=%d total bytes, %d ms, %f B/s", itemsPerBatch, numBatches, itemSize, numThreads, totalBytes, end - start, getBps));
		
		out.println(String.format("%d\t%d\t%d\t%d\t\t\t\t\t%f\t%f", numThreads, itemSize, itemsPerBatch, numBatches, insertBps, getBps));
		out.flush();
	}

	static final int OP_READ = 1;
	static final int OP_WRITE = 2;
	
	PerfTestAccumulo(String tableName, int itemSize, int itemsPerBatch, int numBatches, int op) {
		this.tableName = tableName;
		this.itemSize = itemSize;
		this.itemsPerBatch = itemsPerBatch;
		this.numBatches = numBatches;
		this.op = op;
	}
	Connector connector = null;
	MultiTableBatchWriter mtbw = null;
	String tableName = null;
	int itemSize;
	int itemsPerBatch;
	int numBatches;
	int op;
	int numSplits = 20;
	public ArrayList<byte[]> keys = new ArrayList<byte[]>();
	
	public MultiTableBatchWriter connect() throws AccumuloException, AccumuloSecurityException {
		String zkMasterAddress = "10.0.1.179";
		String user = "root";
		String password = "osdns124p8";
		//tableName = "perftest";
		ZooKeeperInstance zki = new ZooKeeperInstance("accumulo", zkMasterAddress);

		connector = zki.getConnector(user, new PasswordToken(password));
		BatchWriterConfig bwc = new BatchWriterConfig();
		mtbw = connector.createMultiTableBatchWriter(bwc);
		/*
			ClientOnRequiredTable opts = new ClientOnRequiredTable();
			BatchWriterOpts bwOpts = new BatchWriterOpts();
			opts.parseArgs(PerfTestAccumulo.class.getName(), args, bwOpts);

			connector = opts.getConnector();
			mtbw = connector.createMultiTableBatchWriter(bwOpts.getBatchWriterConfig());
			tableName = opts.tableName;
		 */

		return mtbw;
	}
	public void ensureTable(String tableName) throws AccumuloException, AccumuloSecurityException {
		if (connector == null) {
			connect();
		}
		TableOperations tops = connector.tableOperations();
		if (!tops.exists(tableName)) {
			try{
				tops.create(tableName);
				SortedSet<Text> splits = new TreeSet<Text>();
				int splitsize = (0x7fffffff / numSplits) * 2;
				for (int i = 1; i < numSplits; i++) {
					splits.add(new Text(intToBytesBE(splitsize * i)));
				}
				tops.addSplits(tableName, splits);
				tops.setProperty(tableName, "table.bloom.enabled", "true");
				tops.setProperty(tableName, "table.cache.block.enabled", "true");
				tops.setProperty(tableName, "table.cache.index.enabled", "true");
			} catch (TableExistsException tee) {
				// ok, don't care
			} catch (TableNotFoundException e) {
				// This is highly unlikely, as it immediately follows create()
				e.printStackTrace();
			}
		}
	}
	
	public void run() {
		try {
			if (op == OP_WRITE) {
				testWriting();
			} else if (op == OP_READ) {
				testReading();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	byte[] cf = new byte[]{'d'};
	
	public void testWriting() throws Exception {
		connect();
		BatchWriter bw = mtbw.getBatchWriter(tableName);
		Random rand = new Random();
		byte[] value = new byte[itemSize];
		rand.nextBytes(value);
		byte[] empty = new byte[]{};
		
		// do writing
		for (int batchi = 0; batchi < numBatches; batchi++) {
			for (int i = 0; i < itemsPerBatch; i++) {
				byte[] key = new byte[8];
				rand.nextBytes(key);
				keys.add(key);
				Mutation m = new Mutation(key);
				m.put(cf, empty, value);
				bw.addMutation(m);
			}
			mtbw.flush();
			//bw.flush(); // "Must flush all tables, can not flush an individual table"
		}
		mtbw.close();
	}
	
	public static byte[] intToBytesBE(int x) {
		/* The Java Way!
		ByteArrayOutputStream bos = new ByteArrayOutputStream(4);
		DataOutputStream dos = new DataOutputStream(bos);
		try {
			dos.writeInt(x);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return bos.toByteArray();
		*/
		// My Way
		byte[] out = new byte[4];
		out[0] = (byte)((x >> 24) & 0x0ff);
		out[1] = (byte)((x >> 16) & 0x0ff);
		out[2] = (byte)((x >>  8) & 0x0ff);
		out[3] = (byte)((x      ) & 0x0ff);
		return out;
	}
	
	public static void testIntToBytesBE() {
		byte[] ta = intToBytesBE(0x7fffaa33);
		assert ta[0] == 0x7f;
		assert ta[1] == 0xff;
		assert ta[2] == 0xaa;
		assert ta[3] == 0x33;
	}
	
	public void testReading() throws Exception {
		if (connector == null) {
			connect();
		}
		Scanner sc = connector.createScanner(tableName, new Authorizations());
		sc.fetchColumnFamily(new Text(cf));
		for (byte[] key : keys) {
			sc.setRange(new Range(new Text(key)));
			boolean any = false;
			for (Map.Entry kv : sc) {
				byte[] val = ((Value)kv.getValue()).get();
				if (val.length != itemSize) {
					throw new Exception(String.format("wanted item size %d got %d", itemSize, val.length));
				}
			}
		}
	}
}
