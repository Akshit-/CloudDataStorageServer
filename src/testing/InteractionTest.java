package testing;

import java.io.IOException;

import junit.framework.TestCase;
import logger.LogSetup;

import org.apache.log4j.Level;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import client.KVStore;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import ecs.ECServer;

//Running test cases in order of method names in ascending order It is not required here because each method is independent
/*@FixMethodOrder(MethodSorters.NAME_ASCENDING)*/
public class InteractionTest extends TestCase {

	private KVStore kvClient;
	private ECServer mECServer;

	@Before
	public void setUp() {
		
		try {
			new LogSetup("logs/testing/InteractionTest.log", Level.DEBUG);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		mECServer = new ECServer("ecstest.config");
		mECServer.initService(1);
		mECServer.start();
		kvClient = new KVStore("localhost", 50000);

		try {

			kvClient.connect();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
	@After
	public void tearDown() {
		
		kvClient.disconnect();
		mECServer.shutDown();
	}

	@AfterClass
	public void shutDownECServer() {
		
		
		
	}
	/**
	 * Testing put of client. It performs marshalling and calls remote put method to store the tuple.
	 * @asserttrue if no exception occured and the response status is PUT_SUCCESS
	 */
	@Test
	public void testPut() {
		String key = "foo";
		String value = "bar";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
	}
	
	/**
	 * Testing put of client if the client is disconnected
	 * @assertNotNull if exception occurs
	 */
	@Test
	public void testPutDisconnected() {
		kvClient.disconnect();
		String key = "foo";
		String value = "bar";
		Exception ex = null;

		try {
			kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertNotNull(ex);
	}
	
	/**
	 * Test on updating a tuple in storage
	 * @assertTrue if no exception while storing the data && response status equals PUT_UPDATED and the value is updated
	 */
	@Test
	public void testUpdate() {
		String key = "updateTestValue";
		String initialValue = "initial";
		String updatedValue = "updated";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, initialValue);
			response = kvClient.put(key, updatedValue);
			
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE
				&& response.getValue().equals(updatedValue));
	}
	
	/**
	 * Testing client on deletion of a tuple from the storage
	 * Initially, client puts a key/value pair and then deletes it
	 */
	@Test
	public void testDelete() {
		String key = "deleteTestValue";
		String value = "toDelete";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient.put(key, "null");
			
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
	}
	
	/**
	 * Testing GET of KVStore. Firstly, client puts a tuple and then gets it.
	 * @asserttrue if get is successful
	 */
	@Test
	public void testGet() {
		String key = "foobar";
		String value = "bar";
		KVMessage response = null;
		Exception ex = null;

			try {
				kvClient.put(key, value);
				response = kvClient.get(key);
			} catch (Exception e) {
				ex = e;
			}
		
		assertTrue(ex == null && response.getValue().equals("bar"));
	}

	/**
	 * Testing GET of KVStore when the value is not stored.
	 * @asserttrue if get is not successful
	 */
	@Test
	public void testGetUnsetValue() {
		String key = "an unset value";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
	}
}
