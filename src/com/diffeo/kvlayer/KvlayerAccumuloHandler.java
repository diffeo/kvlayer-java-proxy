package com.diffeo.kvlayer;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Handles a kvlayer api client connected to CborRpcServer.
 * There will be a new instance of this for each socket accepted by the server.
 * 
 * This is a statefull interface, so the per-connection object is also the RpcHandler.
 * This object maintains a connection to an Accumulo cluster based on the "connect" message from the client.
 * 
 * @author bolson
 *
 */
public class KvlayerAccumuloHandler extends CborClientHandler implements RpcHandler {
	private static final Logger logger = LogManager.getLogger("com.diffeo.kvlayer.KvlayerAccumuloHandler");
	
	public KvlayerAccumuloHandler() {
		
	}
	public KvlayerAccumuloHandler(Socket client, RpcHandler logic) {
		super();
		init(client, this);
	}
	@Override
	public void init(Socket client, RpcHandler logic) {
		this.client = client;
		this.logic = this;
	}
	// Accumulo connection from "connect" message
	ZooKeeperInstance zki = null;
	Connector connector = null;
	// What was set in "setup_namespace"
	HashMap<String, Object> tableOpts = new HashMap<String, Object>();
	HashMap<String, String> valueTypes = new HashMap<String, String>();

	/**
	 * Because ZooKeeperInstance.getConnector() can hang forever, run it in a thread that we can kill when we decide to timeout.
	 * @author bolson
	 *
	 */
	public static class ConnectThread implements Runnable {
		public Connector cn = null;
		public String user;
		public String password;
		public ZooKeeperInstance zki;
		public Exception err = null;

		public ConnectThread(ZooKeeperInstance zki, String user, String password) {
			this.zki = zki;
			this.user = user;
			this.password = password;
		}

		public void run() {
			try {
				cn = zki.getConnector(user, new PasswordToken(password));
			} catch (AccumuloException e) {
				logger.error("error connecting to accumulo");
				e.printStackTrace();
				err = e;
			} catch (AccumuloSecurityException e) {
				logger.error("error connecting to accumulo");
				e.printStackTrace();
				err = e;
			}
			synchronized(this) {
				this.notify();
			}
		}
	}

	Object connect(List<Object> params) throws AccumuloException, AccumuloSecurityException {
		String zkMasterAddress = (String) params.get(0);
		String user = (String) params.get(1);
		String password = (String) params.get(2);
		String instanceName = (String) params.get(3);
		zki = new ZooKeeperInstance(instanceName, zkMasterAddress);
		ConfigurationCopy conf = new ConfigurationCopy(zki.getConfiguration());
		conf.set(Property.GENERAL_RPC_TIMEOUT, "10s"); // default 120s
		zki.setConfiguration(conf);
		/*
		// noisy debug of default and modified configuration
		for (Entry<String, String> x : conf) {
			logger.debug("conf: " + x.getKey() + " : " + x.getValue());
		}
		*/

		// this can hang forever. Put in a thread that can be killed.
		//connector = zki.getConnector(user, new PasswordToken(password));
		ConnectThread ct = new ConnectThread(zki, user, password);
		Thread ctt = new Thread(ct);
		ctt.start();
		try {
			synchronized(ct) {
				ct.wait(10000); // 10s
			}
		} catch (InterruptedException e) {
			logger.error("interrupted waiting for accumulo zki connect");
			e.printStackTrace();
			return new Object[]{false,"interrupted waiting for accumulo zki connect"};
		}
		if (ct.cn == null) {
			ctt.interrupt();
			String msg = "no connection (timeout)";
			if (ct.err != null) {
				StringWriter sw = new StringWriter();
				PrintWriter pw = new PrintWriter(sw);
				ct.err.printStackTrace(pw);
				msg = msg + ":\n" + sw.toString();
			}
			return new Object[]{false,msg};
		}
		connector = ct.cn;
		return new Object[]{true, null};
	}
	
	@Override
	protected void close() {
		// would call zki.close() and connector.close() if those existed.
		connector = null;
		zki = null;
		tableOpts = null;
		super.close();
	}

	public Object handle(String method, List<Object> params) throws Exception {
		logger.debug(method);
		if (method.equals("connect")) {
			// connect to Accumulo with parameters.
			return connect(params);
		}
		if (method.equals("shutdown")) {
			// something special for testing. should probably be disabled in production.
			logger.error("shutdown message receieved. exit()ing.");
			// TODO: graceful shutdown. stop accepting new connections, exit when all clients have disconnected.
			System.exit(0);
		}
		if (connector == null) {
			throw new Exception("not connected, call \"connect\" method");
		}
		if (method.equals("setup_namespace")) {
			// create tables
			return setupNamespace(params);
		}
		if (method.equals("delete_namespace")) {
			// delete tables
			return deleteNamespace(params);
		}
		if (method.equals("clear_table")) {
			// delete and re-create one table
			return clearTable(params);
		}
		// row data operations:
		if (method.equals("put")) {
			return put(params, true);
		}
		if (method.equals("increment")) {
			return increment(params);
		}
		if (method.equals("scan")) {
			return scan(params);
		}
		if (method.equals("scan_keys")) {
			return scanKeys(params);
		}
		if (method.equals("get")) {
			return get(params);
		}
		if (method.equals("delete")) {
			return delete(params);
		}

		throw new Exception("unknown method: " + method);
	}
	
	// We use the column family "" for data in our simple key-value table.
	private static final byte[] cf = new byte[0];
	private static final byte[] cq = new byte[0];

	int scanLimitItems = 500;
	long scanLimitBytes = 10000000;

	/**
	 * Scan tables returning data.
	 * params should be first the table name to scan, then a list of low-high pairs of ranges to scan.
	 * Range low or high may be null to indicate -Inf/+Inf
	 * @param params [tableName, [[range low, range high], ...] ]
	 * @return [[key, value], ...] OR {'out':[...], 'next':[...]}
	 * @throws TableNotFoundException
	 */
	private Object scan(List<Object> params) throws TableNotFoundException {
		String tableName = (String) params.get(0);
		//logger.info("scan " + tableName);
		Scanner sc = connector.createScanner(tableName, new Authorizations());
		sc.fetchColumnFamily(new Text(cf));
		List rangeList = (List)params.get(1);
		ArrayList<Object[]> out = new ArrayList<Object[]>();
		int outCount = 0;
		long outBytes = 0;
		// TODO: lots of shared structure with scanKeys() refactor to merge?
		for (int rangei = 0; rangei < rangeList.size(); rangei++) {
			List rangel = (List) rangeList.get(rangei);
			byte[] startKey = (byte[]) rangel.get(0);
			byte[] endKey = (byte[]) rangel.get(1);
			boolean startInclusive = true;
			if ((rangel.size() >= 3) && (rangel.get(2).equals("gt"))) {
				startInclusive = false;
			}
			//logger.info("range: " + startKey + " - " + endKey);
			Text skt = null;
			if (startKey != null) {
				skt = new Text(startKey);
			}
			Text ekt = null;
			if (endKey != null) {
				ekt = new Text(endKey);
			}
			Range sr = new Range(skt, startInclusive, ekt, true);
			sc.setRange(sr);
			for (Map.Entry kv : sc) {
				byte[] key = ((Key)kv.getKey()).getRow().getBytes();
				byte[] val = ((Value)kv.getValue()).get();
				out.add(new Object[]{key, val});
				outCount++;
				outBytes += key.length + val.length;
				if ((outCount >= scanLimitItems) || (outBytes >= scanLimitBytes)) {
					Map<String, Object> mout = new HashMap<String,Object>();
					mout.put("out", out);
					ArrayList<Object> nextCommand = new ArrayList<Object>();
					nextCommand.add(tableName);
					ArrayList<Object> newRanges = new ArrayList<Object>();
					ArrayList<Object> currentRange = new ArrayList<Object>();
					currentRange.add(key);
					currentRange.add(endKey);
					currentRange.add("gt");
					newRanges.add(currentRange);
					for (int xi = rangei+1; xi < rangeList.size(); xi++) {
						newRanges.add(rangeList.get(xi));
					}
					nextCommand.add(newRanges);
					mout.put("next", nextCommand);
					return mout;
				}
			}
		}
		//logger.info("found " + out.size() + " things scanned");
		return out;
	}

	/**
	 * Scan tables returning keys only.
	 * params should be first the table name to scan, then a list of low-high pairs of ranges to scan.
	 * Range low or high may be null to indicate -Inf/+Inf
	 * @param params [tableName, [[range low, range high], ...] ]
	 * @return [[key, value], ...]
	 * @throws TableNotFoundException
	 */
	private Object scanKeys(List<Object> params) throws TableNotFoundException {
		// TODO: use org.apache.accumulo.core.iterators.SortedKeyIterator to move key-only into the accumulo side.
		// On the plus side, the current implementation does at least save one hop of not forwarding the data.
		String tableName = (String) params.get(0);
		//logger.info("scan " + tableName);
		Scanner sc = connector.createScanner(tableName, new Authorizations());
		sc.fetchColumnFamily(new Text(cf));
		List rangeList = (List)params.get(1);
		ArrayList<Object> out = new ArrayList<Object>();
		int outCount = 0;
		long outBytes = 0;
		// TODO: lots of shared structure with scan() refactor to merge?
		for (int rangei = 0; rangei < rangeList.size(); rangei++) {
			List rangel = (List) rangeList.get(rangei);
			byte[] startKey = (byte[]) rangel.get(0);
			byte[] endKey = (byte[]) rangel.get(1);
			boolean startInclusive = true;
			if ((rangel.size() >= 3) && (rangel.get(2).equals("gt"))) {
				startInclusive = false;
			}
			//logger.info("range: " + startKey + " - " + endKey);
			Text skt = null;
			if (startKey != null) {
				skt = new Text(startKey);
			}
			Text ekt = null;
			if (endKey != null) {
				ekt = new Text(endKey);
			}
			Range sr = new Range(skt, startInclusive, ekt, true);
			sc.setRange(sr);
			for (Map.Entry kv : sc) {
				byte[] key = ((Key)kv.getKey()).getRow().getBytes();
				out.add(key);
				outCount++;
				outBytes += key.length;
				if ((outCount >= scanLimitItems) || (outBytes >= scanLimitBytes)) {
					Map<String, Object> mout = new HashMap<String,Object>();
					mout.put("out", out);
					ArrayList<Object> nextCommand = new ArrayList<Object>();
					nextCommand.add(tableName);
					ArrayList<Object> newRanges = new ArrayList<Object>();
					ArrayList<Object> currentRange = new ArrayList<Object>();
					currentRange.add(key);
					currentRange.add(endKey);
					currentRange.add("gt");
					newRanges.add(currentRange);
					for (int xi = rangei+1; xi < rangeList.size(); xi++) {
						newRanges.add(rangeList.get(xi));
					}
					nextCommand.add(newRanges);
					mout.put("next", nextCommand);
					return mout;
				}
			}
		}
		//logger.info("found " + out.size() + " things scanned");
		return out;
	}

	/**
	 * Get values for specific keys.
	 * @param params [tableName, [key, ...]]
	 * @return [[key, val], ...]
	 * @throws TableNotFoundException
	 */
	private Object get(List<Object> params) throws TableNotFoundException {
		String tableName = (String) params.get(0);
		Scanner sc = connector.createScanner(tableName, new Authorizations());
		sc.fetchColumnFamily(new Text(cf));
		List keyList = (List)params.get(1);
		ArrayList<Object[]> out = new ArrayList<Object[]>();
		for (Object okey : keyList) {
			byte[] key = (byte[]) okey;
			sc.setRange(new Range(new Text(key)));
			boolean any = false;
			for (Map.Entry kv : sc) {
				byte[] val = ((Value)kv.getValue()).get();
				out.add(new Object[]{key, val});
				any = true;
				break;
			}
			if (!any) {
				out.add(new Object[]{key, null});
			}
		}
		return out;
	}

	/**
	 * Put data.
	 * @param params [tableName, [[key, val], ...] ]
	 * @return true
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws TableNotFoundException
	 */
	private Object put(List<Object> params, boolean deleteCounterFirst) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		String tableName = (String) params.get(0);
		List kvList = (List) params.get(1);
		if (deleteCounterFirst) {
			String valueType = valueTypes.get(tableName);
			if (valueType != null && valueType.equals("COUNTER")) {
				putDeletesForKvList(tableName, kvList);
			}
		}
		BatchWriterConfig bwc = new BatchWriterConfig();
		BatchWriter bw = connector.createBatchWriter(tableName, bwc);
		for (Object ob : kvList) {
			List kv = (List) ob;
			byte[] key = (byte[]) kv.get(0);
			byte[] val = (byte[]) kv.get(1);
			Mutation m = new Mutation(key);
			m.put(cf, cq, val);
			bw.addMutation(m);
		}
		bw.flush();
		bw.close();
		return true;
	}

	private void putDeletesForKvList(String tableName, List kvList) throws TableNotFoundException, MutationsRejectedException {
		BatchWriterConfig bwc = new BatchWriterConfig();
		BatchWriter bw = connector.createBatchWriter(tableName, bwc);
		for (Object ob : kvList) {
			List kv = (List) ob;
			byte[] key = (byte[]) kv.get(0);
			Mutation m = new Mutation(key);
			m.putDelete(cf, cq);
			bw.addMutation(m);
		}
		bw.flush();
		bw.close();
	}

	private Object increment(List<Object> params) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		return put(params, false);
	}

	/**
	 * Delete rows.
	 * @param params [tableName, [key, ...]]
	 * @return true
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws TableNotFoundException
	 */
	private Object delete(List<Object> params) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		String tableName = (String) params.get(0);
		BatchWriterConfig bwc = new BatchWriterConfig();
		BatchWriter bw = connector.createBatchWriter(tableName, bwc);
		List kList = (List) params.get(1);
		for (Object ob : kList) {
			byte[] key = (byte[]) ob;
			Mutation m = new Mutation(key);
			m.putDelete(cf, cq);
			bw.addMutation(m);
		}
		bw.flush();
		bw.close();
		return true;
	}
	
	/**
	 * Delete all table contents. (destroy table and re-create it)
	 * @param params [tableName]
	 * @return true
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws TableNotFoundException
	 */
	private Object clearTable(List<Object> params) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		String tableName = (String)params.get(0);
		TableOperations tops = connector.tableOperations();
		deleteTable(tops, tableName);
		createTable(tops, tableName, valueTypes.get(tableName));
		return true;
	}
	
	/**
	 * Delete all tables created by "setup_namespace".
	 * @param params none
	 * @return
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws TableNotFoundException
	 */
	private Object deleteNamespace(List<Object> params) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		TableOperations tops = connector.tableOperations();
		for (String key : tableOpts.keySet()) {
			deleteTable(tops, key);
		}
		return true;
	}

	private void deleteTable(TableOperations tops, String tableName) throws AccumuloException, AccumuloSecurityException {
		if (!tops.exists(tableName)) {
			return;
		}
		try {
			tops.delete(tableName);
		} catch (TableNotFoundException e) {
			// okay
		}
	}
	
	private void createTable(TableOperations tops, String tableName, String typeName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
		if (tops.exists(tableName)) {
			logger.debug("createTable " + tableName + " - " + typeName + " -- exists");
			return;
		}
		try {
			tops.create(tableName);
		} catch (TableExistsException tee) {
			// okay.
			logger.debug("createTable " + tableName + " - " + typeName + " -- create collision");
			return;
		}
		logger.debug("createTable " + tableName + " - " + typeName);

		tops.setProperty(tableName, "table.bloom.enabled", "true");
		tops.setProperty(tableName, "table.cache.block.enable", "true");
		if (typeName != null && typeName.equals("COUNTER")) {
			logger.debug("setting up COUNTER table " + tableName);
			EnumSet<IteratorUtil.IteratorScope> scopes = EnumSet.of(
					IteratorUtil.IteratorScope.scan,
					IteratorUtil.IteratorScope.minc,
					IteratorUtil.IteratorScope.majc);
			IteratorSetting iters = new IteratorSetting(10, (Class<? extends SortedKeyValueIterator<Key, Value>>) SummingCombiner.class);
			iters.addOption("all", "true");
			iters.addOption("columns", "");
			iters.addOption("type", "FIXEDLEN");
			tops.attachIterator(tableName, iters, scopes);
		}
	}
	
	/**
	 * Create several tables.
	 * Takes one param which is a Map from tableName String to table params map (table params map is currently unused).
	 * @param params [{tableName:{table params (unused)}, ...}]
	 * @return
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 * @throws TableExistsException
	 * @throws TableNotFoundException
	 */
	private Object setupNamespace(List<Object> params) throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
		Map tableSetup = (Map)params.get(0);
		Map tableValueSetup = null;
		if (params.size() > 1) {
			tableValueSetup = (Map)params.get(1);
		}
		TableOperations tops = connector.tableOperations();
		for (Object okey : tableSetup.keySet()) {
			String key = (String)okey;
			String typeName = null;
			if (tableValueSetup != null) {
				typeName = (String)tableValueSetup.get(key);
				if (typeName != null) {
					valueTypes.put(key, typeName);
				}
			}
			createTable(tops, key, typeName);
			tableOpts.put(key, tableSetup.get(key));
		}
		return true;
	}

	public static void main(String[] args) {
		CborRpcServer server = new CborRpcServer();
		server.parseArgs(args);
		server.handlerClass = KvlayerAccumuloHandler.class;
		try {
			server.run();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
	}
}
