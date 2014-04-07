package testing;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import junit.framework.TestCase;
import logger.LogSetup;

import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.Test;

import testing.StorageUtils;
import client.KVStore;
import ecs.ECServer;

public class PerformanceTest extends TestCase{

	private HashMap<String, String> data;
	private ECServer mECServer;

	@Before
	public void setUp() {
		try {
			new LogSetup("logs/testing/PerformanceTest.log", Level.OFF);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		System.out.println("PerformanceTest::setUp()");
		File folder = new File("Enron_Mail_Data");
		try {
			System.out.println("storing the Enron dataset");
			data = StorageUtils.storeDataSet(folder);
			System.out.println("DONE");
		} catch (Exception e) {
			e.printStackTrace();
		}

		mECServer = new ECServer("ecstest.config");

	}

	@SuppressWarnings("unchecked")
	@Test
	public void test1Client1Server() {

		mECServer.initService(1);
		mECServer.start();

		System.out.println("PerformanceTest:: 1 Clients - 1 ServerNode");
		KVStore kvClient = new KVStore("localhost", 50000);

		try {

			kvClient.connect();
		} catch (Exception e) {
			e.printStackTrace();
		}

		int stop = 0;
		Iterator<?> it = data.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, String> pairs = (Map.Entry<String, String>) it.next();
			if (stop == 100) break;
			stop++;
			try {
				kvClient.put(pairs.getKey(), pairs.getValue());

				assertEquals(pairs.getValue(), kvClient.get(pairs.getKey()).getValue());
				//assertTrue(pairs.getValue().equals(kvClient.get(pairs.getKey())));

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		kvClient.disconnect();
		mECServer.shutDown();
		kvClient.displayLatencyStatistics("kvClient0");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void test5Clients1Server() {

		mECServer.initService(1);
		mECServer.start();

		System.out.println("PerformanceTest:: 5 Clients - 1 ServerNode");
		KVStore kvClient = new KVStore("localhost", 50000);
		KVStore kvClient1 = new KVStore("localhost", 50000);
		KVStore kvClient2 = new KVStore("localhost", 50000);
		KVStore kvClient3 = new KVStore("localhost", 50000);
		KVStore kvClient4 = new KVStore("localhost", 50000);

		try {

			kvClient.connect();
			kvClient1.connect();
			kvClient2.connect();
			kvClient3.connect();
			kvClient4.connect();
		} catch (Exception e) {
			e.printStackTrace();
		}

		int stop = 0;

		Iterator<?> it = data.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, String> pairs = (Map.Entry<String, String>) it
					.next();

			if (stop == 100) break;
			stop++;

			try {
				kvClient.put(pairs.getKey(), pairs.getValue());

				assertEquals(pairs.getValue(), kvClient1.get(pairs.getKey()).getValue());

				kvClient2.put(pairs.getKey(), pairs.getValue());

				assertEquals(pairs.getValue(), kvClient2.get(pairs.getKey()).getValue());

				kvClient4.put(pairs.getKey(), pairs.getValue());

				assertEquals(pairs.getValue(), kvClient3.get(pairs.getKey()).getValue());

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		kvClient.displayLatencyStatistics("kvClient0");
		kvClient1.displayLatencyStatistics("kvClient1");
		kvClient2.displayLatencyStatistics("kvClient2");
		kvClient3.displayLatencyStatistics("kvClient3");
		kvClient4.displayLatencyStatistics("kvClient4");

		kvClient.disconnect();
		kvClient1.disconnect();
		kvClient2.disconnect();
		kvClient3.disconnect();
		kvClient4.disconnect();
		mECServer.shutDown();
		//when a client disconnects the servernode displays 
		//its statistics
	}

	//10 client 1 server
	@SuppressWarnings("unchecked")
	@Test
	public void test10Clients1Server() {

		mECServer.initService(1);
		mECServer.start();


		System.out.println("PerformanceTest:: 10 Clients - 1 ServerNode");
		KVStore kvClient = new KVStore("localhost", 50000);
		KVStore kvClient1 = new KVStore("localhost", 50000);
		KVStore kvClient2 = new KVStore("localhost", 50000);
		KVStore kvClient3 = new KVStore("localhost", 50000);
		KVStore kvClient4 = new KVStore("localhost", 50000);
		KVStore kvClient5 = new KVStore("localhost", 50000);
		KVStore kvClient6 = new KVStore("localhost", 50000);
		KVStore kvClient7 = new KVStore("localhost", 50000);
		KVStore kvClient8 = new KVStore("localhost", 50000);
		KVStore kvClient9 = new KVStore("localhost", 50000);

		try {

			kvClient.connect();
			kvClient1.connect();
			kvClient2.connect();
			kvClient3.connect();
			kvClient4.connect();
			kvClient5.connect();
			kvClient6.connect();
			kvClient7.connect();
			kvClient8.connect();
			kvClient9.connect();
		} catch (Exception e) {
			e.printStackTrace();
		}

		int stop = 0;

		Iterator<?> it = data.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, String> pairs = (Map.Entry<String, String>) it
					.next();

			if (stop == 100 ) break;
			stop++;

			try {
				kvClient.put(pairs.getKey(), pairs.getValue());
				assertEquals(pairs.getValue(), kvClient1.get(pairs.getKey()).getValue());

				kvClient2.put(pairs.getKey(), pairs.getValue());
				assertEquals(pairs.getValue(), kvClient3.get(pairs.getKey()).getValue());

				kvClient4.put(pairs.getKey(), pairs.getValue());
				assertEquals(pairs.getValue(), kvClient5.get(pairs.getKey()).getValue());

				kvClient6.put(pairs.getKey(), pairs.getValue());
				assertEquals(pairs.getValue(), kvClient7.get(pairs.getKey()).getValue());

				kvClient8.put(pairs.getKey(), pairs.getValue());
				assertEquals(pairs.getValue(), kvClient9.get(pairs.getKey()).getValue());

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		kvClient.displayLatencyStatistics("kvClient0");
		kvClient1.displayLatencyStatistics("kvClient1");
		kvClient2.displayLatencyStatistics("kvClient2");
		kvClient3.displayLatencyStatistics("kvClient3");
		kvClient4.displayLatencyStatistics("kvClient4");
		kvClient5.displayLatencyStatistics("kvClient5");
		kvClient6.displayLatencyStatistics("kvClient6");
		kvClient7.displayLatencyStatistics("kvClient7");
		kvClient8.displayLatencyStatistics("kvClient8");
		kvClient9.displayLatencyStatistics("kvClient9");

		kvClient.disconnect();
		kvClient1.disconnect();
		kvClient2.disconnect();
		kvClient3.disconnect();
		kvClient4.disconnect();
		kvClient5.disconnect();
		kvClient6.disconnect();
		kvClient7.disconnect();
		kvClient8.disconnect();
		kvClient9.disconnect();
		mECServer.shutDown();
		//when a client disconnects the servernode displays 
		//its statistics
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void test1Client3Server() {

		mECServer.initService(3);
		mECServer.start();

		System.out.println("PerformanceTest:: 1 Clients - 3 ServerNode");
		KVStore kvClient = new KVStore("localhost", 50000);

		try {

			kvClient.connect();
		} catch (Exception e) {
			e.printStackTrace();
		}
		int stop = 0;
		Iterator<?> it = data.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, String> pairs = (Map.Entry<String, String>) it.next();

			//System.out.println("Putting "+pairs.getKey()+", "+pairs.getValue());

			if (stop == 100) break;
			stop++;
			try {
				kvClient.put(pairs.getKey(), pairs.getValue());
				assertEquals(pairs.getValue(), kvClient.get(pairs.getKey()).getValue());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		kvClient.disconnect();
		mECServer.shutDown();
		kvClient.displayLatencyStatistics("kvClient0");
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void test5Clients3Server() {

		mECServer.initService(3);
		mECServer.start();


		System.out.println("PerformanceTest:: 5 Clients - 3 ServerNode");
		KVStore kvClient = new KVStore("localhost", 50000);
		KVStore kvClient1 = new KVStore("localhost", 50000);
		KVStore kvClient2 = new KVStore("localhost", 50000);
		KVStore kvClient3 = new KVStore("localhost", 50000);
		KVStore kvClient4 = new KVStore("localhost", 50000);

		try {

			kvClient.connect();
			kvClient1.connect();
			kvClient2.connect();
			kvClient3.connect();
			kvClient4.connect();
		} catch (Exception e) {
			e.printStackTrace();
		}

		int stop = 0;

		Iterator<?> it = data.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, String> pairs = (Map.Entry<String, String>) it
					.next();

			if (stop == 10) break;
			stop++;

			try {
				kvClient.put(pairs.getKey(), pairs.getValue());
				assertEquals(pairs.getValue(), kvClient1.get(pairs.getKey()).getValue());

				kvClient2.put(pairs.getKey(), pairs.getValue());
				assertEquals(pairs.getValue(), kvClient3.get(pairs.getKey()).getValue());

				kvClient4.put(pairs.getKey(), pairs.getValue());
				assertEquals(pairs.getValue(), kvClient.get(pairs.getKey()).getValue());

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		kvClient.displayLatencyStatistics("kvClient0");
		kvClient1.displayLatencyStatistics("kvClient1");
		kvClient2.displayLatencyStatistics("kvClient2");
		kvClient3.displayLatencyStatistics("kvClient3");
		kvClient4.displayLatencyStatistics("kvClient4");

		kvClient.disconnect();
		kvClient1.disconnect();
		kvClient2.disconnect();
		kvClient3.disconnect();
		kvClient4.disconnect();
		mECServer.shutDown();
		//when a client disconnects the servernode displays 
		//its statistics
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void test10Clients3Server() {

		mECServer.initService(3);
		mECServer.start();


		System.out.println("PerformanceTest:: 10 Clients - 3 ServerNode");
		KVStore kvClient = new KVStore("localhost", 50000);
		KVStore kvClient1 = new KVStore("localhost", 50000);
		KVStore kvClient2 = new KVStore("localhost", 50000);
		KVStore kvClient3 = new KVStore("localhost", 50000);
		KVStore kvClient4 = new KVStore("localhost", 50000);
		KVStore kvClient5 = new KVStore("localhost", 50000);
		KVStore kvClient6 = new KVStore("localhost", 50000);
		KVStore kvClient7 = new KVStore("localhost", 50000);
		KVStore kvClient8 = new KVStore("localhost", 50000);
		KVStore kvClient9 = new KVStore("localhost", 50000);

		try {

			kvClient.connect();
			kvClient1.connect();
			kvClient2.connect();
			kvClient3.connect();
			kvClient4.connect();
			kvClient5.connect();
			kvClient6.connect();
			kvClient7.connect();
			kvClient8.connect();
			kvClient9.connect();
		} catch (Exception e) {
			e.printStackTrace();
		}

		int stop = 0;

		Iterator<?> it = data.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, String> pairs = (Map.Entry<String, String>) it
					.next();

			if (stop == 10) break;
			stop++;

			try {
				kvClient.put(pairs.getKey(), pairs.getValue());
				assertEquals(pairs.getValue(), kvClient1.get(pairs.getKey()).getValue());

				kvClient2.put(pairs.getKey(), pairs.getValue());
				assertEquals(pairs.getValue(), kvClient3.get(pairs.getKey()).getValue());

				kvClient4.put(pairs.getKey(), pairs.getValue());
				assertEquals(pairs.getValue(), kvClient5.get(pairs.getKey()).getValue());				

				kvClient6.put(pairs.getKey(), pairs.getValue());
				assertEquals(pairs.getValue(), kvClient7.get(pairs.getKey()).getValue());				

				kvClient8.put(pairs.getKey(), pairs.getValue());
				assertEquals(pairs.getValue(), kvClient9.get(pairs.getKey()).getValue());

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		kvClient.displayLatencyStatistics("kvClient0");
		kvClient1.displayLatencyStatistics("kvClient1");
		kvClient2.displayLatencyStatistics("kvClient2");
		kvClient3.displayLatencyStatistics("kvClient3");
		kvClient4.displayLatencyStatistics("kvClient4");
		kvClient5.displayLatencyStatistics("kvClient5");
		kvClient6.displayLatencyStatistics("kvClient6");
		kvClient7.displayLatencyStatistics("kvClient7");
		kvClient8.displayLatencyStatistics("kvClient8");
		kvClient9.displayLatencyStatistics("kvClient9");

		kvClient.disconnect();
		kvClient1.disconnect();
		kvClient2.disconnect();
		kvClient3.disconnect();
		kvClient4.disconnect();
		kvClient5.disconnect();
		kvClient6.disconnect();
		kvClient7.disconnect();
		kvClient8.disconnect();
		kvClient9.disconnect();
		mECServer.shutDown();
		//when a client disconnects the servernode displays 
		//its statistics
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void test1Client5Server() {

		mECServer.initService(5);
		mECServer.start();

		System.out.println("PerformanceTest:: 1 Clients - 5 ServerNode");
		KVStore kvClient = new KVStore("localhost", 50000);

		try {

			kvClient.connect();
		} catch (Exception e) {
			e.printStackTrace();
		}
		int stop = 0;
		Iterator<?> it = data.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, String> pairs = (Map.Entry<String, String>) it.next();

			//System.out.println("Putting "+pairs.getKey()+", "+pairs.getValue());

			if (stop == 10) break;
			stop++;
			try {
				kvClient.put(pairs.getKey(), pairs.getValue());
				assertEquals(pairs.getValue(), kvClient.get(pairs.getKey()).getValue());

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		kvClient.disconnect();
		mECServer.shutDown();
		kvClient.displayLatencyStatistics("kvClient0");
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void test5Clients5Server() {

		mECServer.initService(5);
		mECServer.start();


		System.out.println("PerformanceTest:: 5 Clients - 5 ServerNode");
		KVStore kvClient = new KVStore("localhost", 50000);
		KVStore kvClient1 = new KVStore("localhost", 50000);
		KVStore kvClient2 = new KVStore("localhost", 50000);
		KVStore kvClient3 = new KVStore("localhost", 50000);
		KVStore kvClient4 = new KVStore("localhost", 50000);

		try {

			kvClient.connect();
			kvClient1.connect();
			kvClient2.connect();
			kvClient3.connect();
			kvClient4.connect();
		} catch (Exception e) {
			e.printStackTrace();
		}

		int stop = 0;

		Iterator<?> it = data.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, String> pairs = (Map.Entry<String, String>) it
					.next();

			if (stop == 10) break;
			stop++;

			try {
				kvClient.put(pairs.getKey(), pairs.getValue());
				assertEquals(pairs.getValue(), kvClient1.get(pairs.getKey()).getValue());

				kvClient2.put(pairs.getKey(), pairs.getValue());
				assertEquals(pairs.getValue(), kvClient3.get(pairs.getKey()).getValue());

				kvClient4.put(pairs.getKey(), pairs.getValue());
				assertEquals(pairs.getValue(), kvClient.get(pairs.getKey()).getValue());

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		kvClient.displayLatencyStatistics("kvClient0");
		kvClient1.displayLatencyStatistics("kvClient1");
		kvClient2.displayLatencyStatistics("kvClient2");
		kvClient3.displayLatencyStatistics("kvClient3");
		kvClient4.displayLatencyStatistics("kvClient4");

		kvClient.disconnect();
		kvClient1.disconnect();
		kvClient2.disconnect();
		kvClient3.disconnect();
		kvClient4.disconnect();
		mECServer.shutDown();
		//when a client disconnects the servernode displays 
		//its statistics
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void test10Clients5Server() {

		mECServer.initService(5);
		mECServer.start();


		System.out.println("PerformanceTest:: 10 Clients - 5 ServerNode");
		KVStore kvClient = new KVStore("localhost", 50000);
		KVStore kvClient1 = new KVStore("localhost", 50000);
		KVStore kvClient2 = new KVStore("localhost", 50000);
		KVStore kvClient3 = new KVStore("localhost", 50000);
		KVStore kvClient4 = new KVStore("localhost", 50000);
		KVStore kvClient5 = new KVStore("localhost", 50000);
		KVStore kvClient6 = new KVStore("localhost", 50000);
		KVStore kvClient7 = new KVStore("localhost", 50000);
		KVStore kvClient8 = new KVStore("localhost", 50000);
		KVStore kvClient9 = new KVStore("localhost", 50000);

		try {

			kvClient.connect();
			kvClient1.connect();
			kvClient2.connect();
			kvClient3.connect();
			kvClient4.connect();
			kvClient5.connect();
			kvClient6.connect();
			kvClient7.connect();
			kvClient8.connect();
			kvClient9.connect();
		} catch (Exception e) {
			e.printStackTrace();
		}

		int stop = 0;

		Iterator<?> it = data.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, String> pairs = (Map.Entry<String, String>) it
					.next();

			if (stop == 10 ) break;
			stop++;

			try {
				kvClient.put(pairs.getKey(), pairs.getValue());
				assertEquals(pairs.getValue(), kvClient1.get(pairs.getKey()).getValue());

				kvClient2.put(pairs.getKey(), pairs.getValue());
				assertEquals(pairs.getValue(), kvClient3.get(pairs.getKey()).getValue());

				kvClient4.put(pairs.getKey(), pairs.getValue());
				assertEquals(pairs.getValue(), kvClient5.get(pairs.getKey()).getValue());

				kvClient6.put(pairs.getKey(), pairs.getValue());
				assertEquals(pairs.getValue(), kvClient6.get(pairs.getKey()).getValue());

				kvClient8.put(pairs.getKey(), pairs.getValue());
				assertEquals(pairs.getValue(), kvClient9.get(pairs.getKey()).getValue());

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		kvClient.displayLatencyStatistics("kvClient0");
		kvClient1.displayLatencyStatistics("kvClient1");
		kvClient2.displayLatencyStatistics("kvClient2");
		kvClient3.displayLatencyStatistics("kvClient3");
		kvClient4.displayLatencyStatistics("kvClient4");
		kvClient5.displayLatencyStatistics("kvClient5");
		kvClient6.displayLatencyStatistics("kvClient6");
		kvClient7.displayLatencyStatistics("kvClient7");
		kvClient8.displayLatencyStatistics("kvClient8");
		kvClient9.displayLatencyStatistics("kvClient9");

		kvClient.disconnect();
		kvClient1.disconnect();
		kvClient2.disconnect();
		kvClient3.disconnect();
		kvClient4.disconnect();
		kvClient5.disconnect();
		kvClient6.disconnect();
		kvClient7.disconnect();
		kvClient8.disconnect();
		kvClient9.disconnect();
		mECServer.shutDown();
		//when a client disconnects the servernode displays 
		//its statistics
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void test1Client8Server() {

		mECServer.initService(5);
		mECServer.start();

		System.out.println("PerformanceTest:: 1 Clients - 8 ServerNode");
		KVStore kvClient = new KVStore("localhost", 50000);

		try {

			kvClient.connect();
		} catch (Exception e) {
			e.printStackTrace();
		}
		int stop = 0;
		Iterator<?> it = data.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, String> pairs = (Map.Entry<String, String>) it.next();

			//System.out.println("Putting "+pairs.getKey()+", "+pairs.getValue());

			if (stop == 10) break;
			stop++;
			try {
				kvClient.put(pairs.getKey(), pairs.getValue());
				assertEquals(pairs.getValue(), kvClient.get(pairs.getKey()).getValue());

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		kvClient.disconnect();
		mECServer.shutDown();
		kvClient.displayLatencyStatistics("kvClient0");
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void test5Clients8Server() {

		mECServer.initService(8);
		mECServer.start();


		System.out.println("PerformanceTest:: 5 Clients - 8 ServerNode");
		KVStore kvClient = new KVStore("localhost", 50000);
		KVStore kvClient1 = new KVStore("localhost", 50000);
		KVStore kvClient2 = new KVStore("localhost", 50000);
		KVStore kvClient3 = new KVStore("localhost", 50000);
		KVStore kvClient4 = new KVStore("localhost", 50000);

		try {

			kvClient.connect();
			kvClient1.connect();
			kvClient2.connect();
			kvClient3.connect();
			kvClient4.connect();
		} catch (Exception e) {
			e.printStackTrace();
		}

		int stop = 0;

		Iterator<?> it = data.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, String> pairs = (Map.Entry<String, String>) it
					.next();

			if (stop == 10) break;
			stop++;

			try {
				kvClient.put(pairs.getKey(), pairs.getValue());
				assertEquals(pairs.getValue(), kvClient1.get(pairs.getKey()).getValue());

				kvClient2.put(pairs.getKey(), pairs.getValue());
				assertEquals(pairs.getValue(), kvClient3.get(pairs.getKey()).getValue());

				kvClient4.put(pairs.getKey(), pairs.getValue());
				assertEquals(pairs.getValue(), kvClient.get(pairs.getKey()).getValue());

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		kvClient.displayLatencyStatistics("kvClient0");
		kvClient1.displayLatencyStatistics("kvClient1");
		kvClient2.displayLatencyStatistics("kvClient2");
		kvClient3.displayLatencyStatistics("kvClient3");
		kvClient4.displayLatencyStatistics("kvClient4");

		kvClient.disconnect();
		kvClient1.disconnect();
		kvClient2.disconnect();
		kvClient3.disconnect();
		kvClient4.disconnect();
		mECServer.shutDown();
		//when a client disconnects the servernode displays 
		//its statistics
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void test10Clients8Server() {

		mECServer.initService(8);
		mECServer.start();


		System.out.println("PerformanceTest:: 10 Clients - 8 ServerNode");
		KVStore kvClient = new KVStore("localhost", 50000);
		KVStore kvClient1 = new KVStore("localhost", 50000);
		KVStore kvClient2 = new KVStore("localhost", 50000);
		KVStore kvClient3 = new KVStore("localhost", 50000);
		KVStore kvClient4 = new KVStore("localhost", 50000);
		KVStore kvClient5 = new KVStore("localhost", 50000);
		KVStore kvClient6 = new KVStore("localhost", 50000);
		KVStore kvClient7 = new KVStore("localhost", 50000);
		KVStore kvClient8 = new KVStore("localhost", 50000);
		KVStore kvClient9 = new KVStore("localhost", 50000);

		try {

			kvClient.connect();
			kvClient1.connect();
			kvClient2.connect();
			kvClient3.connect();
			kvClient4.connect();
			kvClient5.connect();
			kvClient6.connect();
			kvClient7.connect();
			kvClient8.connect();
			kvClient9.connect();
		} catch (Exception e) {
			e.printStackTrace();
		}

		int stop = 0;

		Iterator<?> it = data.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, String> pairs = (Map.Entry<String, String>) it
					.next();

			if (stop == 10) break;
			stop++;

			try {
				kvClient.put(pairs.getKey(), pairs.getValue());
				assertEquals(pairs.getValue(), kvClient1.get(pairs.getKey()).getValue());

				kvClient2.put(pairs.getKey(), pairs.getValue());
				assertEquals(pairs.getValue(), kvClient3.get(pairs.getKey()).getValue());

				kvClient4.put(pairs.getKey(), pairs.getValue());
				assertEquals(pairs.getValue(), kvClient5.get(pairs.getKey()).getValue());

				kvClient6.put(pairs.getKey(), pairs.getValue());
				assertEquals(pairs.getValue(), kvClient7.get(pairs.getKey()).getValue());

				kvClient8.put(pairs.getKey(), pairs.getValue());
				assertEquals(pairs.getValue(), kvClient9.get(pairs.getKey()).getValue());

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		kvClient.displayLatencyStatistics("kvClient0");
		kvClient1.displayLatencyStatistics("kvClient1");
		kvClient2.displayLatencyStatistics("kvClient2");
		kvClient3.displayLatencyStatistics("kvClient3");
		kvClient4.displayLatencyStatistics("kvClient4");
		kvClient5.displayLatencyStatistics("kvClient5");
		kvClient6.displayLatencyStatistics("kvClient6");
		kvClient7.displayLatencyStatistics("kvClient7");
		kvClient8.displayLatencyStatistics("kvClient8");
		kvClient9.displayLatencyStatistics("kvClient9");

		kvClient.disconnect();
		kvClient1.disconnect();
		kvClient2.disconnect();
		kvClient3.disconnect();
		kvClient4.disconnect();
		kvClient5.disconnect();
		kvClient6.disconnect();
		kvClient7.disconnect();
		kvClient8.disconnect();
		kvClient9.disconnect();
		mECServer.shutDown();
		//when a client disconnects the servernode displays 
		//its statistics
	}


	@Test
	public void testThroughput5Clients1Server() {

		mECServer.initService(1);
		mECServer.start();


		System.out.println("PerformanceTest:: 5 Clients - 1 ServerNode");
		final KVStore kvClient = new KVStore("localhost", 50000);
		final KVStore kvClient1 = new KVStore("localhost", 50000);
		final KVStore kvClient2 = new KVStore("localhost", 50000);
		final KVStore kvClient3 = new KVStore("localhost", 50000);
		final KVStore kvClient4 = new KVStore("localhost", 50000);


		try {

			kvClient.connect();
			kvClient1.connect();
			kvClient2.connect();
			kvClient3.connect();
			kvClient4.connect();

		} catch (Exception e) {
			e.printStackTrace();
		}

		ArrayList<Thread> threads = new ArrayList<Thread>();

		Thread t1 =		new Thread(){
			@Override
			public void run(){
				try {
					int stop = 0;
					Iterator<Entry<String, String>> it = data.entrySet().iterator();
					while (it.hasNext()) {
						final Map.Entry<String, String> pairs = it
								.next();

						if (stop == 10 ) break;
						stop++;
						kvClient.put(pairs.getKey(), pairs.getValue());
						assertEquals(pairs.getValue(), kvClient.get(pairs.getKey()).getValue());

					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				kvClient.displayLatencyStatistics("kvClient0");
				kvClient.disconnect();
			}
		};
		t1.start();
		threads.add(t1);

		Thread t2 =		new Thread(){
			@Override
			public void run(){
				try {
					int stop = 0;
					Iterator<Entry<String, String>> it = data.entrySet().iterator();
					while (it.hasNext()) {
						final Map.Entry<String, String> pairs = it
								.next();

						if (stop == 10 ) break;
						stop++;
						kvClient1.put(pairs.getKey(), pairs.getValue());
						assertEquals(pairs.getValue(), kvClient1.get(pairs.getKey()).getValue());
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				kvClient1.displayLatencyStatistics("kvClient1");
				kvClient1.disconnect();
			}
		};
		t2.start();
		threads.add(t2);

		Thread t3 = new Thread(){
			@Override
			public void run(){
				try {
					int stop = 0;
					Iterator<Entry<String, String>> it = data.entrySet().iterator();
					while (it.hasNext()) {
						final Map.Entry<String, String> pairs = it
								.next();

						if (stop == 10 ) break;
						stop++;
						kvClient2.put(pairs.getKey(), pairs.getValue());
						assertEquals(pairs.getValue(), kvClient2.get(pairs.getKey()).getValue());
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				kvClient2.displayLatencyStatistics("kvClient2");
				kvClient2.disconnect();

			}
		};
		t3.start();
		threads.add(t3);

		Thread t4 = 	new Thread(){
			@Override
			public void run(){
				try {
					int stop = 0;
					Iterator<Entry<String, String>> it = data.entrySet().iterator();
					while (it.hasNext()) {
						final Map.Entry<String, String> pairs = it
								.next();

						if (stop == 10) break;
						stop++;
						kvClient3.put(pairs.getKey(), pairs.getValue());
						assertEquals(pairs.getValue(), kvClient3.get(pairs.getKey()).getValue());
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				kvClient3.displayLatencyStatistics("kvClient3");
				kvClient3.disconnect();
			}
		};

		t4.start();
		threads.add(t4);

		for(Thread t:threads){
			try {
				t.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		mECServer.shutDown();


	}
	// 10 client 1 server
	@Test
	public void testThroughput10Clients1Server() {

		mECServer.initService(1);
		mECServer.start();


		System.out.println("PerformanceTest:: 10 Clients - 1 ServerNode");
		final KVStore kvClient = new KVStore("localhost", 50000);
		final KVStore kvClient1 = new KVStore("localhost", 50000);
		final KVStore kvClient2 = new KVStore("localhost", 50000);
		final KVStore kvClient3 = new KVStore("localhost", 50000);
		final KVStore kvClient4 = new KVStore("localhost", 50000);
		final KVStore kvClient5 = new KVStore("localhost", 50000);
		final KVStore kvClient6 = new KVStore("localhost", 50000);
		final KVStore kvClient7 = new KVStore("localhost", 50000);
		final KVStore kvClient8 = new KVStore("localhost", 50000);
		final KVStore kvClient9 = new KVStore("localhost", 50000);

		try {

			kvClient.connect();
			kvClient1.connect();
			kvClient2.connect();
			kvClient3.connect();
			kvClient4.connect();
			kvClient5.connect();
			kvClient6.connect();
			kvClient7.connect();
			kvClient8.connect();
			kvClient9.connect();
		} catch (Exception e) {
			e.printStackTrace();
		}


		ArrayList<Thread> threads = new ArrayList<Thread>();

		Thread t1 =		new Thread(){
			@Override
			public void run(){
				try {
					int stop = 0;
					Iterator<Entry<String, String>> it = data.entrySet().iterator();
					while (it.hasNext()) {
						final Map.Entry<String, String> pairs = it
								.next();

						if (stop == 10) break;
						stop++;
						kvClient.put(pairs.getKey(), pairs.getValue());
						assertEquals(pairs.getValue(), kvClient.get(pairs.getKey()).getValue());
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				kvClient.displayLatencyStatistics("kvClient0");
				kvClient.disconnect();
			}
		};
		t1.start();
		threads.add(t1);

		Thread t2 =		new Thread(){
			@Override
			public void run(){
				try {
					int stop = 0;
					Iterator<Entry<String, String>> it = data.entrySet().iterator();
					while (it.hasNext()) {
						final Map.Entry<String, String> pairs = it
								.next();

						if (stop == 10 ) break;
						stop++;
						kvClient1.put(pairs.getKey(), pairs.getValue());
						assertEquals(pairs.getValue(), kvClient1.get(pairs.getKey()).getValue());

					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				kvClient1.displayLatencyStatistics("kvClient1");
				kvClient1.disconnect();
			}
		};
		t2.start();
		threads.add(t2);

		Thread t3 = new Thread(){
			@Override
			public void run(){
				try {
					int stop = 0;
					Iterator<Entry<String, String>> it = data.entrySet().iterator();
					while (it.hasNext()) {
						final Map.Entry<String, String> pairs = it
								.next();

						if (stop == 10 ) break;
						stop++;
						kvClient2.put(pairs.getKey(), pairs.getValue());
						assertEquals(pairs.getValue(), kvClient2.get(pairs.getKey()).getValue());
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				kvClient2.displayLatencyStatistics("kvClient2");
				kvClient2.disconnect();

			}
		};
		t3.start();
		threads.add(t3);

		Thread t4 = 	new Thread(){
			@Override
			public void run(){
				try {
					int stop = 0;
					Iterator<Entry<String, String>> it = data.entrySet().iterator();
					while (it.hasNext()) {
						final Map.Entry<String, String> pairs = it
								.next();

						if (stop == 10 ) break;
						stop++;
						kvClient3.put(pairs.getKey(), pairs.getValue());
						assertEquals(pairs.getValue(), kvClient3.get(pairs.getKey()).getValue());
					
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				kvClient3.displayLatencyStatistics("kvClient3");
				kvClient3.disconnect();
			}
		};

		t4.start();
		threads.add(t4);

		Thread t5= new Thread(){
			@Override
			public void run(){
				try {
					int stop = 0;
					Iterator<Entry<String, String>> it = data.entrySet().iterator();
					while (it.hasNext()) {
						final Map.Entry<String, String> pairs = it
								.next();

						if (stop == 10 ) break;
						stop++;
						kvClient4.put(pairs.getKey(), pairs.getValue());
						assertEquals(pairs.getValue(), kvClient4.get(pairs.getKey()).getValue());
					
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				kvClient4.displayLatencyStatistics("kvClient4");
				kvClient4.disconnect();
			}
		};
		t5.start();
		threads.add(t5);

		Thread t6 = new Thread(){
			@Override
			public void run(){
				try {
					int stop = 0;
					Iterator<Entry<String, String>> it = data.entrySet().iterator();
					while (it.hasNext()) {
						final Map.Entry<String, String> pairs = it
								.next();

						if (stop == 10 ) break;
						stop++;
						kvClient5.put(pairs.getKey(), pairs.getValue());
						assertEquals(pairs.getValue(), kvClient5.get(pairs.getKey()).getValue());
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				kvClient5.displayLatencyStatistics("kvClient5");
				kvClient5.disconnect();
			}
		};
		t6.start();
		threads.add(t6);

		Thread t7 = new Thread(){
			@Override
			public void run(){
				try {
					int stop = 0;
					Iterator<Entry<String, String>> it = data.entrySet().iterator();
					while (it.hasNext()) {
						final Map.Entry<String, String> pairs = it
								.next();

						if (stop == 10 ) break;
						stop++;
						kvClient6.put(pairs.getKey(), pairs.getValue());
						assertEquals(pairs.getValue(), kvClient6.get(pairs.getKey()).getValue());
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				kvClient6.displayLatencyStatistics("kvClient6");
				kvClient6.disconnect();

			}
		};
		t7.start();
		threads.add(t7);

		Thread t8 = new Thread(){
			@Override
			public void run(){
				try {
					int stop = 0;
					Iterator<Entry<String, String>> it = data.entrySet().iterator();
					while (it.hasNext()) {
						final Map.Entry<String, String> pairs = it
								.next();

						if (stop == 10 ) break;
						stop++;
						kvClient7.put(pairs.getKey(), pairs.getValue());
						assertEquals(pairs.getValue(), kvClient7.get(pairs.getKey()).getValue());
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				kvClient7.displayLatencyStatistics("kvClient7");
				kvClient7.disconnect();

			}
		};
		t8.start();
		threads.add(t8);

		Thread t9 = new Thread(){
			@Override
			public void run(){
				try {
					int stop = 0;
					Iterator<Entry<String, String>> it = data.entrySet().iterator();
					while (it.hasNext()) {
						final Map.Entry<String, String> pairs = it
								.next();

						if (stop == 10 ) break;
						stop++;
						kvClient8.put(pairs.getKey(), pairs.getValue());
						assertEquals(pairs.getValue(), kvClient8.get(pairs.getKey()).getValue());
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				kvClient8.displayLatencyStatistics("kvClient8");
				kvClient8.disconnect();

			}
		};
		t9.start();
		threads.add(t9);

		Thread t10 = new Thread(){
			@Override
			public void run(){
				try {
					int stop = 0;
					Iterator<Entry<String, String>> it = data.entrySet().iterator();
					while (it.hasNext()) {
						final Map.Entry<String, String> pairs = it
								.next();

						if (stop == 10 ) break;
						stop++;
						kvClient9.put(pairs.getKey(), pairs.getValue());
						assertEquals(pairs.getValue(), kvClient9.get(pairs.getKey()).getValue());
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				kvClient9.displayLatencyStatistics("kvClient9");
				kvClient9.disconnect();
			}

		};
		t10.start();
		threads.add(t10);

		for(Thread t:threads){
			try {
				t.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		mECServer.shutDown();
		//when a client disconnects the servernode displays 
		//its statistics
	}

}
