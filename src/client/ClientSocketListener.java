package client;

import common.messages.TextMessage;

/** 
 * This class is used to send messages to be printed on GUI
 * 
 */
public interface ClientSocketListener {
	
	/**
	 * Enum for of Client Connection states(CONNECTED, DISCONNECTED, CONNECTION_LOST).
	 *
	 */
	public enum SocketStatus{CONNECTED, DISCONNECTED, CONNECTION_LOST};
	
	/**
	 * Method to handle message received from the server.
	 * @param msg 
	 * 			Message that is received.
	 */
	public void handleNewMessage(TextMessage msg);
	
	/**
	 * Method to display information to user according to Client connection state.
	 * @param status 
	 * 				Current status of Client connection.
	 */
	public void handleStatus(SocketStatus status);
}
