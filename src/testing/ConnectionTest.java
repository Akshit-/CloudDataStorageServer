package testing;

import java.net.UnknownHostException;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import client.KVStore;
import ecs.ECServer;

public class ConnectionTest extends TestCase {

	ECServer mECServer;

	@Before
	public void setUp() {
		mECServer = new ECServer("ecstest.config");
		mECServer.initService(1);
		mECServer.start();
	}

	@After 
	public void tearDown() {
		mECServer.shutDown();
		mECServer=null;
	}

	/**
	 * kvClient is trying to connect to server listening on a specific port
	 * Testing if the connection is established
	 * @assert if exception is null the connection is established
	 */
	@Test
	public void testConnectionSuccess() {

		Exception ex = null;

		KVStore kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e;

			ex.printStackTrace();

		}

		assertNull(ex);
	}

	/**
	 * kvClient trying to connect to unknown host
	 * 
	 * @assert Testing if UnknownHostException is thrown
	 */
	@Test
	public void testUnknownHost() {
		Exception ex = null;
		KVStore kvClient = new KVStore("unknown", 50000);

		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex instanceof UnknownHostException);
	}

	/**
	 * kvClient trying to connect to server using illegal port
	 * @assert Testing if IllegalArgumentException is thrown
	 */
	@Test
	public void testIllegalPort() {
		Exception ex = null;
		KVStore kvClient = new KVStore("localhost", 123456789);

		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex instanceof IllegalArgumentException);
	}
}
