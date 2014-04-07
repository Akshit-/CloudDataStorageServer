package testing;

import java.io.IOException;

import junit.framework.TestCase;
import logger.LogSetup;

import org.apache.log4j.Level;
import org.junit.BeforeClass;
import org.junit.Test;

import common.messages.JSONSerializer;
import common.messages.KVMessage.StatusType;
import common.messages.KVMessageImpl;
import common.messages.TextMessage;

public class MarshallingTest extends TestCase {

	@BeforeClass
	public void testLogSetup() throws Exception {
		Exception ex=null;
		try {
			new LogSetup("logs/testing/MarshallingTest.log", Level.DEBUG);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			ex=e1;
			e1.printStackTrace();
		}
		assertNull(ex);
	}

	/**
	 * Testing if JSON marshalling of PUT request performed as expected
	 */
	@Test
	public void testPut() {
		TextMessage txtMsg = JSONSerializer.marshal("host", "localhost",
				StatusType.PUT);
		TextMessage expected = new TextMessage(
				"{\"key\":\"host\",\"value\":\"localhost\",\"status\":3}");
		
		assertEquals(txtMsg.getMsg(), expected.getMsg());
	}
	
	/**
	 * Testing if JSON marshalling of GET request performed as expected
	 */
	@Test
	public void testGet() {
		KVMessageImpl kvmsg = JSONSerializer.unMarshal(new TextMessage(
				"{\"key\":\"host\",\"value\":\"localhost\",\"status\":0}"));
		KVMessageImpl expected = new KVMessageImpl("host", "localhost",
				StatusType.GET);
		assertEquals(kvmsg.getKey(), expected.getKey());
		assertEquals(kvmsg.getValue(), expected.getValue());
		assertEquals(kvmsg.getStatus(), expected.getStatus());
	}

	/**
	 * Testing if statusType message is being marshalled as intended
	 */
	@Test
	public void testType() {

		String key = "test";
		String value = "qwerty";
		
		KVMessageImpl msg1 = new KVMessageImpl(key, value, StatusType.PUT_SUCCESS);
		KVMessageImpl msg2 = new KVMessageImpl(key, value, StatusType.PUT_UPDATE);
		KVMessageImpl msg3 = new KVMessageImpl(key, value, StatusType.GET);
		KVMessageImpl msg4 = new KVMessageImpl(key, value, StatusType.GET_ERROR);
		KVMessageImpl msg5 = new KVMessageImpl(key, value, StatusType.GET_SUCCESS);
		KVMessageImpl msg6 = new KVMessageImpl(key, value, StatusType.PUT);
		KVMessageImpl msg7 = new KVMessageImpl(key, value, StatusType.PUT_ERROR);
		
		TextMessage txtMsg1 = JSONSerializer.marshal(msg1.getKey(),msg1.getValue(),msg1.getStatus());
		TextMessage txtMsg2 = JSONSerializer.marshal(msg2.getKey(),msg2.getValue(),msg2.getStatus());
		TextMessage txtMsg3 = JSONSerializer.marshal(msg3.getKey(),msg3.getValue(),msg3.getStatus());
		TextMessage txtMsg4 = JSONSerializer.marshal(msg4.getKey(),msg4.getValue(),msg4.getStatus());
		TextMessage txtMsg5 = JSONSerializer.marshal(msg5.getKey(),msg5.getValue(),msg5.getStatus());
		TextMessage txtMsg6 = JSONSerializer.marshal(msg6.getKey(),msg6.getValue(),msg6.getStatus());
		TextMessage txtMsg7 = JSONSerializer.marshal(msg7.getKey(),msg7.getValue(),msg7.getStatus());
		
		try {
			KVMessageImpl kv1 = JSONSerializer.unMarshal(txtMsg1);
			assertTrue(msg1.getStatus().equals(kv1.getStatus()));
			
			KVMessageImpl kv2 = JSONSerializer.unMarshal(txtMsg2);
			assertTrue(msg2.getStatus().equals(kv2.getStatus()));
			
			KVMessageImpl kv3 = JSONSerializer.unMarshal(txtMsg3);
			assertTrue(msg3.getStatus().equals(kv3.getStatus()));
			
			KVMessageImpl kv4 = JSONSerializer.unMarshal(txtMsg4);
			assertTrue(msg4.getStatus().equals(kv4.getStatus()));
			
			KVMessageImpl kv5 = JSONSerializer.unMarshal(txtMsg5);
			assertTrue(msg5.getStatus().equals(kv5.getStatus()));
			
			KVMessageImpl kv6 = JSONSerializer.unMarshal(txtMsg6);
			assertTrue(msg6.getStatus().equals(kv6.getStatus()));

			KVMessageImpl kv7 = JSONSerializer.unMarshal(txtMsg7);
			assertTrue(msg7.getStatus().equals(kv7.getStatus()));
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}
