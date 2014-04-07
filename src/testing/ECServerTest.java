package testing;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ecs.ECServer;
import junit.framework.TestCase;

public class ECServerTest extends TestCase {

	private ECServer mECServer;
	@Before
	public void setUp(){
		mECServer = new ECServer("ecstest.config");
	}
	@After
	public void tearDown(){
		if(mECServer!=null){
			mECServer.shutDown();
			mECServer = null;
		}
	}	
	@Test
	public void testInitService() {
		boolean result = mECServer.initService(4);
		int nodeCount = mECServer.getActivatedNodeCount();
		assertTrue(result && nodeCount == 4);
	}

	@Test
	public void testStart() {
		boolean result = mECServer.initService(4);
		boolean result2 = mECServer.start();
		int nodeCount = mECServer.getActivatedNodeCount();
		assertTrue(result && result2 && nodeCount == 4);
	}

	@Test
	public void testStop() {
		boolean result = mECServer.initService(4);
		boolean result2 = mECServer.start();
		boolean result3 = mECServer.stop();
		int nodeCount = mECServer.getActivatedNodeCount();
		assertTrue(result && result2 && result3 && nodeCount == 4);
	}

	@Test
	public void testAddNode() {
		int oldNodeCount = 4;
		boolean result = mECServer.initService(oldNodeCount);
		boolean result2 = mECServer.start();
		boolean result3 = mECServer.addNode();
		int newNodeCount = mECServer.getActivatedNodeCount();
		assertTrue(result && result2 && result3 && newNodeCount == oldNodeCount+1);

	}

	@Test
	public void testRemoveNode() {
		int oldNodeCount = 4;
		boolean result = mECServer.initService(oldNodeCount);
		boolean result2 = mECServer.start();
		boolean result3 = mECServer.removeNode();
		int newNodeCount = mECServer.getActivatedNodeCount();
		assertTrue(result && result2 && result3 && newNodeCount == oldNodeCount-1);
	}

	@Test
	public void testShutdown(){
		int oldNodeCount = 4;
		boolean result = mECServer.initService(oldNodeCount);
		int nodeCountBeforeShutDown = mECServer.getActivatedNodeCount();
		boolean result2 = mECServer.shutDown();
		int nodeCountAfterShutDown = mECServer.getActivatedNodeCount();
		mECServer = null;
		assertTrue(result && result2 && nodeCountBeforeShutDown == oldNodeCount && nodeCountAfterShutDown == 0);
	}
}
