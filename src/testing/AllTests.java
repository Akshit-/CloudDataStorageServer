package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;

public class AllTests {

	static {
		try {
			new LogSetup("logs/testing/test.log", Level.ALL);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage Server Test-Suite");
		clientSuite.addTestSuite(ECServerTest.class);
		clientSuite.addTestSuite(ConnectionTest.class);
		clientSuite.addTestSuite(InteractionTest.class);
		clientSuite.addTestSuite(StorageTest.class);
		clientSuite.addTestSuite(MarshallingTest.class);
		clientSuite.addTestSuite(ServerTest.class);
		clientSuite.addTestSuite(CipherTest.class);
		clientSuite.addTestSuite(PerformanceTest.class);		
		return clientSuite;
	}

}
