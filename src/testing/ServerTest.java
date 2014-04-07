package testing;

import java.io.IOException;

import junit.framework.TestCase;
import logger.LogSetup;

import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import client.KVStore;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import ecs.ECServer;

public class ServerTest extends TestCase {
	private KVStore kvStore1;
	private KVStore kvStore2;
	private KVStore kvStore3;
	private KVStore kvStore4;
	private KVStore kvStore5;
	private KVStore kvStore6;

	private ECServer mECServer;
	@Before
	public void setUp() {
		try {
			new LogSetup("logs/testing/ServerTest.log", Level.ERROR);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		mECServer=new ECServer("ecstest.config");
		mECServer.initService(1);
		mECServer.start();
		initClients();
	}
	
	public void initClients(){
		
			 kvStore1 = new KVStore("localhost", 50000);
			 kvStore2 = new KVStore("localhost", 50000);
			 kvStore3 = new KVStore("localhost", 50000);
			 kvStore4 = new KVStore("localhost", 50000);
			 kvStore5 = new KVStore("localhost", 50000);
			 kvStore6 = new KVStore("localhost", 50000);
	}

	/**
	 * 
	 * Method that tests interaction among multiple clients and the server
	 * Connect, put, get Each kvStore is a separate thread. All run concurrently
	 * sharing the same data structure.
	 * 
	 * @assert if clients have been successfully connected to server && 
	 * clients have been successfully interacting with the server &&
	 *  the final value of tuple is hello5 the test has been passed; otherwise it fails
	 */
	@Test
	public void testMultipleClientsInteraction() {
		Exception ex1 = null;
		Exception ex2 = null;
		KVMessage kvmsg1 = null;
		KVMessage kvmsg2 = null;

		try {
			kvStore1.connect();
			kvStore2.connect();
			kvStore4.connect();
			kvStore6.connect();

		} catch (Exception e) {
			ex1 = e;
		}
		try {

			kvmsg1 = kvStore1.put("hello", "hallo1");
			kvStore3.connect();
			kvmsg2 = kvStore2.get("hello");
			kvmsg1 = kvStore3.put("hello", "hallo3");
			kvmsg2 = kvStore2.get("hello");
			kvmsg1 = kvStore4.put("hello", "hallo4");
			kvmsg2 = kvStore2.get("hello");
			kvStore5.connect();
			kvmsg2 = kvStore5.get("hello");
			kvmsg1 = kvStore3.put("hello", "hallo3");
			kvmsg1 = kvStore5.put("hello", "hallo5");
			kvmsg2 = kvStore2.get("hello");

		} catch (Exception e) {
			ex2 = e;
			e.printStackTrace();
		}
		assertTrue(ex1 == null 
				&& ex2 == null
				&& kvmsg1.getStatus() == StatusType.PUT_UPDATE
				&& kvmsg2.getStatus() == StatusType.GET_SUCCESS
				&& kvmsg2.getValue().equals("hallo5"));
	}
	

	@After
	public void tearDown() {
			kvStore1.disconnect();
			 kvStore2.disconnect();
			 kvStore3.disconnect();
			 kvStore4.disconnect();
			 kvStore5.disconnect();
			 kvStore6.disconnect();
			 mECServer.shutDown();
	}
}
