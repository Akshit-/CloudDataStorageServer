package server;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.math.BigInteger;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import javax.json.Json;
import javax.json.JsonObject;

import metadata.MetaData;

import org.apache.log4j.Logger;

import common.communication.SocketCommunication;
import common.messages.JSONSerializer;
import common.messages.KVAdminMessage;
import common.messages.KVAdminMessage.Commands;
import common.messages.KVAdminMessageImpl;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.KVMessageImpl;
import common.messages.TextMessage;

/**
 * Represents a connection end point for a particular client that is connected
 * to the server and is also responsible for processing message received from ECServer.
 * 
 */
public class ClientConnection implements Runnable {

	private static Logger logger = Logger.getRootLogger();

	private boolean isOpen;

	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;

	private KVServerListener mKVServerListener;
	private ECServerListener mECServerListener;

	//for performance evaluation
	private PerformanceListener mPerfListener;

	//socket communication for sending and receiving socket messages
	private SocketCommunication securedSocketCommunication;

	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * 
	 * @param clientSocket
	 *            the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket) {
		this.clientSocket = clientSocket;
		this.isOpen = true;
		securedSocketCommunication = new SocketCommunication();
	}

	/**
	 * Initializes and starts the client connection. Loops until the connection
	 * is closed or aborted by the client.
	 */
	public void run() {
		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();


			while (isOpen) {
				try {
					TextMessage latestMsg = securedSocketCommunication.receiveMessage(clientSocket);
					logger.info("ClientConnection:: run()-->latestMsg ="+latestMsg);
					if (latestMsg == null) {
						logger.error("ClientConnection:: run()-->latestMsg == null");
						break;
					}

					JsonObject jsonObject = null;
					try {
						jsonObject = Json.createReader(new StringReader(latestMsg.getMsg()))
								.readObject();
					} catch (Exception e) {
						logger.error("ClientConnection:: Exception while converting txtMsg to jsonObject");
					}

					if (jsonObject == null) {
						logger.error("ClientConnection:: run()-->jsonObject == null");
						break;
					}

					if (jsonObject.get("adminMsg")!=null) {						
						logger.info("ClientConnection:: AdminMessage Received");
						KVAdminMessage msg = processKVAdminMessage(latestMsg);
						if(msg!=null){
							logger.info("ClientConnection:: Sending Message to Admin with command:"+msg.getCommand().toString());
							try{
								TextMessage message = JSONSerializer.marshalKVAdminMsg(msg.getCommand());
								logger.info("ClientConnection:: Sending from KVServer:"+message.getMsg().toString());
								securedSocketCommunication.sendMessage(clientSocket,message);
							}catch(Exception e){
								logger.info("ClientConnection:: Could not send from KVServer:"+e);
							}
						}else{
							break;
						}
					}else if(mECServerListener.isActiveForClients()) {
						logger.info("ClientConnection:: KVClient message Received.");	
						KVMessage msg = processKVMessage(latestMsg);	
						if (msg != null) {
							mPerfListener.calculateThroughput(latestMsg.getMsgBytes().length);//evaluation
							if (msg.getStatus().equals(StatusType.SERVER_NOT_RESPONSIBLE)){
								logger.info("ClientConnection::Sending SERVER_NOT_RESPONSIBLE back to KVClient");								
								securedSocketCommunication.sendMessage(clientSocket, new TextMessage(JSONSerializer
										.Marshal(msg)));
							}else{
								logger.info("ClientConnection::Message was handled: "
										+msg.getKey()+","+msg.getValue()+","+msg.getStatus().ordinal());
								securedSocketCommunication.sendMessage(clientSocket, JSONSerializer.marshal(msg));
							}
						} else {
							logger.error("ClientConnection:: msg = null");
							break;
						}


					}else{
						KVMessageImpl kvmessage = JSONSerializer
								.unMarshal(latestMsg);
						securedSocketCommunication.sendMessage(clientSocket,JSONSerializer
								.marshal(kvmessage.getKey(),
										kvmessage.getValue(),
										StatusType.SERVER_STOPPED));
					}

					/*
					 * connection either terminated by the client or lost due to
					 * network problems
					 */
				} catch (IOException ioe) {
					logger.error("Error! Connection lost!");
					isOpen = false;
				}
			}

		} catch (IOException ioe) {
			logger.error("Error! Connection could not be established!", ioe);

		} finally {
			logger.debug("run()-->finally: Closing socket and streams");
			try {
				if (clientSocket != null) {
					input.close();
					output.close();
					clientSocket.close();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
		}

	}

	/**
	 * Processes the client requests and returns the corresponding server's reply
	 * @param request the client request
	 * @return server's reply accordingly to client's request
	 */
	public KVMessageImpl processKVMessage(TextMessage request) {

		KVMessageImpl kvmessage = JSONSerializer.unMarshal(request);

		logger.info("ClientConnection::processKVMessage()+ isLockWrite="+mECServerListener.isLockWrite());

		boolean replicaEnvironment = false;

		//if replica environment exists (should be atleast 3 nodes)
		if(mECServerListener.getServiceMetaData().size()>2){
			replicaEnvironment = true;
		}
		logger.info("ClientConnection::processKVMessage()+ replicaEnvironment="+replicaEnvironment);


		if(kvmessage.getStatus().equals(StatusType.GET)){
			//if replica environment exists (should be atleast 3 nodes)
			if(!replicaEnvironment){

				if (serverNotResponsibleForWrite(kvmessage)){
					logger.info("ClientConnection::processKVMessage() + SERVER_NOT_RESPONSIBLE for key="+kvmessage.getKey());
					return new KVMessageImpl(kvmessage.getKey(),
							kvmessage.getValue(),
							StatusType.SERVER_NOT_RESPONSIBLE, mECServerListener.getServiceMetaData());
				}
			}

			//Check if this server is responsible for current key to retrieve data
			if (replicaEnvironment && serverNotResponsibleForRead(kvmessage)){
				logger.info("ClientConnection::processKVMessage() + SERVER_NOT_RESPONSIBLE for key="+kvmessage.getKey());
				return new KVMessageImpl(kvmessage.getKey(),
						kvmessage.getValue(),
						StatusType.SERVER_NOT_RESPONSIBLE, mECServerListener.getServiceMetaData());

			}			

			String value = mKVServerListener.get(kvmessage.getKey());

			if (value != null) {
				// GET_SUCCESS
				kvmessage = new KVMessageImpl(kvmessage.getKey(), value,
						StatusType.GET_SUCCESS);
				logger.info("GET SUCCESS! Found key=" + kvmessage.getKey()
						+ " on Server with value=" + kvmessage.getValue());

			} else {
				// GET_ERROR
				kvmessage = new KVMessageImpl(kvmessage.getKey(), "",
						StatusType.GET_ERROR);
				logger.info("GET ERROR! Cannot find key=" + kvmessage.getKey()
						+ " on Server");
			}

		} else if (!mECServerListener.isLockWrite() && kvmessage.getStatus().equals(StatusType.PUT)) {
			//We first check whether this KVServer has been locked for writing by Admin (ECSServer).
			
			if(!replicaEnvironment){
				//no replica environment, then we do normal for both put & get
				if (serverNotResponsibleForWrite(kvmessage)){
					logger.info("ClientConnection::processKVMessage() + SERVER_NOT_RESPONSIBLE for key="+kvmessage.getKey());
					return new KVMessageImpl(kvmessage.getKey(),
							kvmessage.getValue(),
							StatusType.SERVER_NOT_RESPONSIBLE, mECServerListener.getServiceMetaData());
				}
			}

			//Check if this server is responsible for current key to write data
			if (replicaEnvironment && serverNotResponsibleForWrite(kvmessage)){
				logger.info("ClientConnection::processKVMessage() + SERVER_NOT_RESPONSIBLE for key="+kvmessage.getKey());
				return new KVMessageImpl(kvmessage.getKey(),
						kvmessage.getValue(),
						StatusType.SERVER_NOT_RESPONSIBLE, mECServerListener.getServiceMetaData());
			}
			
			//Client message to delete this key
			if (kvmessage.getValue().isEmpty()) {
				String previous_value = mKVServerListener.delete(kvmessage
						.getKey());
				if(replicaEnvironment){
					try {

						logger.debug("Replica 1 info "+mECServerListener.getMyReplica1MetaData().getIP()+":"+
								mECServerListener.getMyReplica1MetaData().getPort());
						logger.debug("Replica 1 socket "+mECServerListener.getMyReplica1Socket());
						if(replicaEnvironment){
							//Message to be deleted from Replicas
							KVMessageImpl myReplicaMsg = new KVMessageImpl(kvmessage.getKey(), "", StatusType.REPLICA_PUT);

							//Deleting from replica 1
							securedSocketCommunication.sendMessage(mECServerListener.getMyReplica1Socket(), JSONSerializer.marshal(myReplicaMsg));

							TextMessage txtMsgReply = securedSocketCommunication.receiveMessage(mECServerListener.getMyReplica1Socket());

							KVMessageImpl myReplica1MsgReply = JSONSerializer.unMarshal(txtMsgReply);

							logger.debug("Replica 1 deletion reply:"+txtMsgReply);
							if(myReplica1MsgReply.getStatus().equals(StatusType.REPLICA_DELETE_SUCCESS)) {
								//Replication success
								logger.info("Replication 1 deletion success");
							} else {
								//Replication failure
								logger.error("Replication 1 deletion error");
							}


							logger.debug("Replica 2 info "+mECServerListener.getMyReplica2MetaData().getIP()+":"+
									mECServerListener.getMyReplica2MetaData().getPort());
							logger.debug("Replica 2 socket "+mECServerListener.getMyReplica2Socket());

							//Deleting from replica 2
							securedSocketCommunication.sendMessage(mECServerListener.getMyReplica2Socket(), JSONSerializer.marshal(myReplicaMsg));

							TextMessage txtMsgReply2 = securedSocketCommunication.receiveMessage(mECServerListener.getMyReplica2Socket());

							KVMessageImpl myReplica2MsgReply = JSONSerializer.unMarshal(txtMsgReply2);


							logger.debug("Replica 2 deletion reply:"+txtMsgReply2);
							if(myReplica2MsgReply.getStatus().equals(StatusType.REPLICA_DELETE_SUCCESS)) {
								//Replication success
								logger.info("Replication 2 deletion success");
							} else {
								//Replication failure
								logger.error("Replication 2 deletion error");
							}
						}


					} catch (NumberFormatException e) {
						logger.error("NumberFormatException while replicating delete"+Arrays.toString(e.getStackTrace()));
					} catch (UnknownHostException e) {
						logger.error("UnknownHostException while replicating delete"+Arrays.toString(e.getStackTrace()));
					} catch (IOException e) {
						logger.error("IOException while replicating delete"+Arrays.toString(e.getStackTrace()));
					} catch (Exception e) {
						logger.error("Exception while replicating delete"+Arrays.toString(e.getStackTrace()));
					}
				}

				if (previous_value != null) {
					logger.info("DELETE SUCCESS! Deleted key="
							+ kvmessage.getKey());
					kvmessage = new KVMessageImpl(kvmessage.getKey(),
							previous_value, StatusType.DELETE_SUCCESS);
				} else {
					logger.info("DELETE ERROR! key="
							+ kvmessage.getKey());
					kvmessage = new KVMessageImpl(kvmessage.getKey(),
							kvmessage.getValue(), StatusType.DELETE_ERROR);
				}

			} else {
				//Client request to put the key value pair on server
				String previous_value = mKVServerListener.put(
						kvmessage.getKey(), kvmessage.getValue());

				//Replicating the PUT to replicas
				if(replicaEnvironment){
					try {
						logger.debug("Replica 1 info "+mECServerListener.getMyReplica1MetaData().getIP()+":"+
								mECServerListener.getMyReplica1MetaData().getPort());
						logger.debug("Replica 1 socket "+mECServerListener.getMyReplica1Socket());

						//message to be replicated
						KVMessageImpl myReplicaMsg = new KVMessageImpl(kvmessage.getKey(), kvmessage.getValue(), StatusType.REPLICA_PUT);

						//Copying to replica 1
						securedSocketCommunication.sendMessage(mECServerListener.getMyReplica1Socket(), JSONSerializer.marshal(myReplicaMsg));

						TextMessage txtMsgReply = securedSocketCommunication.receiveMessage(mECServerListener.getMyReplica1Socket());

						KVMessageImpl myReplica1MsgReply = JSONSerializer.unMarshal(txtMsgReply);

						logger.debug("Replica 1 put reply:"+txtMsgReply);
						if(myReplica1MsgReply.getStatus().equals(StatusType.REPLICA_PUT_SUCCESS)
								||myReplica1MsgReply.getStatus().equals(StatusType.REPLICA_PUT_UPDATE)) {
							//Replication success
							logger.info("Replication 1 put success");
						} else {
							//Replication failure
							logger.error("Replication 1 put error");
						}

						logger.debug("Replica 2 info "+mECServerListener.getMyReplica2MetaData().getIP()+":"+
								mECServerListener.getMyReplica2MetaData().getPort());
						logger.debug("Replica 2 socket "+mECServerListener.getMyReplica2Socket());

						//Copying to replica 2	
						securedSocketCommunication.sendMessage(mECServerListener.getMyReplica2Socket(), JSONSerializer.marshal(myReplicaMsg));

						TextMessage txtMsgReply2 = securedSocketCommunication.receiveMessage(mECServerListener.getMyReplica2Socket());

						KVMessageImpl myReplica2MsgReply = JSONSerializer.unMarshal(txtMsgReply2);

						logger.debug("Replica 2 put reply:"+txtMsgReply2);

						if(myReplica2MsgReply.getStatus().equals(StatusType.REPLICA_PUT_SUCCESS)
								||myReplica2MsgReply.getStatus().equals(StatusType.REPLICA_PUT_UPDATE)) {
							//Replication success
							logger.info("Replication 2 put success");
						} else {
							//Replication failure
							logger.error("Replication 2 put error");
						}


					} catch (NumberFormatException e) {
						logger.error("NumberFormatException while replicating PUT"+Arrays.toString(e.getStackTrace()));
					} catch (UnknownHostException e) {
						logger.error("UnknownHostException while replicating PUT"+Arrays.toString(e.getStackTrace()));
					} catch (IOException e) {
						logger.error("IOException while replicating PUT"+Arrays.toString(e.getStackTrace()));
					} catch (Exception e) {
						logger.error("Exception while replicating PUT"+Arrays.toString(e.getStackTrace()));
					}
				}
				if (previous_value != null) {
					// PUT_UPDATE
					// updated previous one
					logger.info("PUT SUCCESS! Updated key="
							+ kvmessage.getKey() + " with new value="
							+ kvmessage.getValue());
					kvmessage = new KVMessageImpl(kvmessage.getKey(),
							kvmessage.getValue(), StatusType.PUT_UPDATE);

				} else {
					// PUT_SUCCESS
					// inserted new one
					logger.info("PUT SUCCESS! Inserted new key="
							+ kvmessage.getKey());
					kvmessage = new KVMessageImpl(kvmessage.getKey(),
							kvmessage.getValue(), StatusType.PUT_SUCCESS);

				}

			}

		} else if (kvmessage.getStatus().equals(StatusType.REPLICA_PUT)) {
			//Coordinator node request to put key-value pair on this replica server
			if (kvmessage.getValue().isEmpty()) {
				String previous_value = mKVServerListener.delete(kvmessage
						.getKey());

				if (previous_value != null) {
					logger.info("Replica DELETE SUCCESS! Deleted key="
							+ kvmessage.getKey());
					kvmessage = new KVMessageImpl(kvmessage.getKey(),
							previous_value, StatusType.REPLICA_DELETE_SUCCESS);
				} else {
					logger.info("Replica DELETE ERROR! key="
							+ kvmessage.getKey());
					kvmessage = new KVMessageImpl(kvmessage.getKey(),
							kvmessage.getValue(), StatusType.REPLICA_DELETE_ERROR);
				}

			} else {
				//Coordinator request to put the key value pair on this replica server
				String previous_value = mKVServerListener.put(
						kvmessage.getKey(), kvmessage.getValue());

				if (previous_value != null) {
					// PUT_UPDATE
					// updated previous one
					logger.info("Replica PUT SUCCESS! Updated key="
							+ kvmessage.getKey() + " with new value="
							+ kvmessage.getValue());
					kvmessage = new KVMessageImpl(kvmessage.getKey(),
							kvmessage.getValue(), StatusType.REPLICA_PUT_UPDATE);

				} else {
					// PUT_SUCCESS
					// inserted new one
					logger.info("Replica PUT SUCCESS! Inserted new key="
							+ kvmessage.getKey());
					kvmessage = new KVMessageImpl(kvmessage.getKey(),
							kvmessage.getValue(), StatusType.REPLICA_PUT_SUCCESS);

				}

			}
		}else if (kvmessage.getStatus().equals(StatusType.DELETE_TOPOLOGICAL)){
			//delete replica2's data
			
			logger.info("processKVMessage() + DELETE_TOPOLOGICAL --> deleting replica2's Data from this server.");
			mECServerListener.deleteDataBetween(mECServerListener.getReplica2MetaData());

		}
		return kvmessage;

	}

	/**
	 * Checks whether the KVServer is Coordinator node for 
	 * key value pair associated with this KVMessage.
	 * 
	 * @param kvmessage
	 * 		KVMessage received for write operation
	 * @return 
	 * 		Returns true if the server is not in charge of the particular request
	 */
	private boolean serverNotResponsibleForWrite(KVMessage kvmessage) {

		BigInteger key = new BigInteger(getMD5(kvmessage.getKey()),16);

		BigInteger startServer = new BigInteger(mECServerListener.getNodeMetaData().getRangeStart(),16);
		BigInteger endServer = new BigInteger(mECServerListener.getNodeMetaData().getRangeEnd(),16);


		BigInteger maximum = new BigInteger("ffffffffffffffffffffffffffffffff",16);

		BigInteger minimum = new BigInteger("00000000000000000000000000000000",16);

		logger.info("ClientConnection::serverNotResponsible() + key="+key
				+", Server's start="+startServer
				+", Server's end="+endServer
				+", Maximum ="+maximum
				+", Minimum ="+minimum);

		if(startServer.compareTo(endServer)<0){
			if (key.compareTo(startServer) > 0 && 
					key.compareTo(endServer) <= 0){

				logger.info("ClientConnection::serverNotResponsible(start<end) + return false");
				return false;
			}
		}else{
			//startServer > endServer
			//keycheck1 = startServer to Maximum && keycheck2 = 0 to end 
			if((key.compareTo(startServer) > 0 && key.compareTo(maximum) <= 0 )
					|| (key.compareTo(minimum) >= 0 && key.compareTo(endServer) <= 0 )){

				logger.info("ClientConnection::serverNotResponsible(start > end) + return false");
				return false;
			}

		}
		logger.info("ClientConnection::serverNotResponsible() + return true");
		return true;
	}

	/**
	 * Checks whether the KVServer is Replica node for 
	 * key value pair associated with this KVMessage.
	 * 
	 * @param kvmessage
	 * 		KVMessage received for read operation
	 * @return 
	 * 		Returns true if the server is not in charge of the particular request
	 */
	private boolean serverNotResponsibleForRead(KVMessage kvmessage) {

		BigInteger key = new BigInteger(getMD5(kvmessage.getKey()),16);

		BigInteger startServer = new BigInteger(mECServerListener.getNodeMetaData().getRangeStart(),16);
		BigInteger endServer = new BigInteger(mECServerListener.getNodeMetaData().getRangeEnd(),16);

		BigInteger startServerReplica1 = new BigInteger(mECServerListener.getReplica1MetaData().getRangeStart(),16);
		BigInteger endServerReplica1 = new BigInteger(mECServerListener.getReplica1MetaData().getRangeEnd(),16);

		BigInteger startServerReplica2 = new BigInteger(mECServerListener.getReplica2MetaData().getRangeStart(),16);
		BigInteger endServerReplica2 = new BigInteger(mECServerListener.getReplica2MetaData().getRangeEnd(),16);

		BigInteger maximum = new BigInteger("ffffffffffffffffffffffffffffffff",16);

		BigInteger minimum = new BigInteger("00000000000000000000000000000000",16);

		logger.info("ClientConnection::serverNotResponsibleForRead()-->Coordinator data + key="+key
				+", Server's start="+startServer
				+", Server's end="+endServer
				+", Maximum ="+maximum
				+", Minimum ="+minimum);

		if(startServer.compareTo(endServer)<0){
			if (key.compareTo(startServer) > 0 && 
					key.compareTo(endServer) <= 0){

				logger.info("ClientConnection::serverNotResponsibleForRead(start<end) + return false");
				return false;
			}
		}else{
			//startServer > endServer
			//keycheck1 = startServer to Maximum && keycheck2 = 0 to end 
			if((key.compareTo(startServer) > 0 && key.compareTo(maximum) <= 0 )
					|| (key.compareTo(minimum) >= 0 && key.compareTo(endServer) <= 0 )){

				logger.info("ClientConnection::serverNotResponsibleForRead(start > end) + return false");
				return false;
			}
		}


		logger.info("ClientConnection::serverNotResponsibleForRead()-->Replica1 + key="+key
				+", Server's start="+startServerReplica1
				+", Server's end="+endServerReplica1
				+", Maximum ="+maximum
				+", Minimum ="+minimum);

		//Check for replica 1
		if(startServerReplica1.compareTo(endServerReplica1)<0){
			if (key.compareTo(startServerReplica1) > 0 && 
					key.compareTo(endServerReplica1) <= 0){

				logger.info("ClientConnection::serverNotResponsibleForRead(start<end) + return false");
				return false;
			}
		}else{
			//startServer > endServer
			//keycheck1 = startServer to Maximum && keycheck2 = 0 to end 
			if((key.compareTo(startServerReplica1) > 0 && key.compareTo(maximum) <= 0 )
					|| (key.compareTo(minimum) >= 0 && key.compareTo(endServerReplica1) <= 0 )){

				logger.info("ClientConnection::serverNotResponsibleForRead(start > end) + return false");
				return false;
			}
		}

		logger.info("ClientConnection::serverNotResponsibleForRead()-->Replica2 + key="+key
				+", Server's start="+startServerReplica2
				+", Server's end="+endServerReplica2
				+", Maximum ="+maximum
				+", Minimum ="+minimum);

		// Check for replica 2
		if(startServerReplica2.compareTo(endServerReplica2)<0){
			if (key.compareTo(startServerReplica2) > 0 && 
					key.compareTo(endServerReplica2) <= 0){

				logger.info("ClientConnection::serverNotResponsibleForRead(startServerReplica2<endServerReplica2) + return false");
				return false;
			}
		}else{
			//startServer > endServer
			//keycheck1 = startServer to Maximum && keycheck2 = 0 to end 
			if((key.compareTo(startServerReplica2) > 0 && key.compareTo(maximum) <= 0 )
					|| (key.compareTo(minimum) >= 0 && key.compareTo(endServerReplica2) <= 0 )){

				logger.info("ClientConnection::serverNotResponsible(startServerReplica2 > endServerReplica2) + return false");
				return false;
			}
		}

		logger.info("ClientConnection::serverNotResponsible() + return true");
		return true;
	}

	/**
	 * Method to compute MD5.
	 * @param msg
	 * 		Message whose MD5 is to be computed.
	 * @return
	 * 		Returns MD5 for the message.
	 */
	private String getMD5(String msg){
		MessageDigest messageDigest = null;
		try {
			messageDigest = MessageDigest.getInstance("MD5");
		} catch(NoSuchAlgorithmException ex){
			logger.info("Exception occurred in MD5: e="+ex);
			return null;
		}

		messageDigest.reset();
		messageDigest.update(msg.getBytes());
		byte[] hashValue = messageDigest.digest();
		BigInteger bigInt = new BigInteger(1,hashValue);
		String hashHex = bigInt.toString(16);
		// Now we need to zero pad it if you actually want the full 32 chars.
		while(hashHex.length() < 32 ){
			hashHex = "0"+hashHex;
		}
		return hashHex;
	}

	/**
	 * Method to process message communication with ECServer.
	 * @param replyMsg
	 * 		Message received from ECServer.
	 * @return
	 * 		Returns KVAdminMessage response after processing the received message. 
	 */
	private KVAdminMessageImpl processKVAdminMessage(TextMessage replyMsg) {

		KVAdminMessageImpl kvAdminMessage = JSONSerializer
				.unmarshalKVAdminMsg(replyMsg);

		if(kvAdminMessage.getCommand().equals(Commands.INIT)){
			logger.info("Executing INIT Command for("+clientSocket.getLocalPort()+")");			
			mECServerListener.initKVServer(kvAdminMessage.getMetaDatas());

			kvAdminMessage = new KVAdminMessageImpl();
			kvAdminMessage.setCommand(Commands.INIT_SUCCESS);

		}else if(kvAdminMessage.getCommand().equals(Commands.UPDATE)){
			logger.info("Executing UPDATE Command for("+clientSocket.getLocalPort()+")");			

			MetaData tempOldMyReplica1 = mECServerListener.getMyReplica1MetaData();
			MetaData tempOldMyReplica2 = mECServerListener.getMyReplica2MetaData();

			mECServerListener.initKVServer(kvAdminMessage.getMetaDatas());

			//Check if myReplicas have changed or not
			if(mECServerListener.getServiceMetaData().size()>2 && tempOldMyReplica1!=null && tempOldMyReplica2!=null){
				
				MetaData tempNewMyReplica1 = mECServerListener.getMyReplica1MetaData();
				MetaData tempNewMyReplica2 = mECServerListener.getMyReplica2MetaData();

				String range = mECServerListener.getNodeMetaData().getRangeStart()+":"+mECServerListener.getNodeMetaData().getRangeEnd();
				if(!tempNewMyReplica1.equals(tempOldMyReplica1) || !tempNewMyReplica1.equals(tempOldMyReplica2)){
					//Checking if 1st successor is new
					logger.info("Successor1 has changed, so Replicating server's data");
					mECServerListener.replicateDataToServer(mECServerListener.getMyReplica1Socket(), range);

				}

				if(!tempNewMyReplica2.equals(tempOldMyReplica1) || !tempNewMyReplica2.equals(tempOldMyReplica2)){
					//Checking if 2nd successor is new
					logger.info("Successor2 has changed, so Replicating server's data");
					mECServerListener.replicateDataToServer(mECServerListener.getMyReplica2Socket(), range);
				}
			}

			kvAdminMessage = new KVAdminMessageImpl();
			kvAdminMessage.setCommand(Commands.UPDATE_SUCCESS);

		}else if(kvAdminMessage.getCommand().equals(Commands.START)){
			logger.info("Executing START Command for("+clientSocket.getLocalPort()+")");
			mECServerListener.startKVServer();

			kvAdminMessage = new KVAdminMessageImpl();
			kvAdminMessage.setCommand(Commands.START_SUCCESS);

		}else if(kvAdminMessage.getCommand().equals(Commands.STOP)){
			logger.info("Excecuting STOP Command for("+clientSocket.getLocalPort()+")");
			mECServerListener.stopKVServer();

			kvAdminMessage = new KVAdminMessageImpl();
			kvAdminMessage.setCommand(Commands.STOP_SUCCESS);

		}else if(kvAdminMessage.getCommand().equals(Commands.SHUTDOWN)){
			logger.info("Executing SHUTDOWN Command for("+clientSocket.getLocalPort()+")");

			if (clientSocket != null) {
				try {
					input.close();
					output.close();
					clientSocket.close();
				} catch (IOException e) {
					logger.info("Exception occurred while executing SHUTDOWN Command: e="+e);
				}

			}
			System.exit(1);
		}else if(kvAdminMessage.getCommand().equals(Commands.LOCK_WRITE)){
			logger.info("Executing LOCK_WRITE Command for("+clientSocket.getLocalPort()+")");
			mECServerListener.lockWrite();

			kvAdminMessage = new KVAdminMessageImpl();
			kvAdminMessage.setCommand(Commands.LOCK_WRITE_SUCCESS);

		}else if(kvAdminMessage.getCommand().equals(Commands.UNLOCK_WRITE)){
			logger.info("Executing UNLOCK_WRITE Command for("+clientSocket.getLocalPort()+")");

			mECServerListener.unlockWrite();

			kvAdminMessage = new KVAdminMessageImpl();
			kvAdminMessage.setCommand(Commands.UNLOCK_WRITE_SUCCESS);

		}else if(kvAdminMessage.getCommand().equals(Commands.MOVE_DATA)){
			logger.info("Executing MOVE_DATA Command for("+clientSocket.getLocalPort()+")");

			String range = kvAdminMessage.getRange();
			String server = kvAdminMessage.getDestinationAddress();

			logger.info("Executing MOVE_DATA Command for("+clientSocket.getLocalPort()+")"
					+", rangestart="+new BigInteger(range.split(":")[0],16)
			+", rangeend="+new BigInteger(range.split(":")[1],16));

			if(mECServerListener.moveData(range,server,false)){
				//MOVE_DATA_SUCCESS
				logger.info("Sending MOVE_DATA_SUCCESS for("+clientSocket.getLocalPort()+")");
				kvAdminMessage = new KVAdminMessageImpl();
				kvAdminMessage.setCommand(Commands.MOVE_DATA_SUCCESS);

			}else{
				//MOVE_DATA_FAIL
				logger.info("Sending MOVE_DATA_FAIL for("+clientSocket.getLocalPort()+")");
				kvAdminMessage = new KVAdminMessageImpl();
				kvAdminMessage.setCommand(Commands.MOVE_DATA_FAIL);
			}			

		}else if(kvAdminMessage.getCommand().equals(Commands.MOVE_DATA_REPLICATE)){
			logger.info("Executing MOVE_DATA Command for("+clientSocket.getLocalPort()+")");

			String range = kvAdminMessage.getRange();
			String server = kvAdminMessage.getDestinationAddress();

			logger.info("Executing MOVE_DATA Command for("+clientSocket.getLocalPort()+")"
					+", rangestart="+new BigInteger(range.split(":")[0],16)
			+", rangeend="+new BigInteger(range.split(":")[1],16));

			if(mECServerListener.moveData(range,server,true)){
				//MOVE_DATA_SUCCESS
				logger.info("Sending MOVE_DATA_SUCCESS for("+clientSocket.getLocalPort()+")");
				kvAdminMessage = new KVAdminMessageImpl();
				kvAdminMessage.setCommand(Commands.MOVE_DATA_SUCCESS);

			}else{
				//MOVE_DATA_FAIL
				logger.info("Sending MOVE_DATA_FAIL for("+clientSocket.getLocalPort()+")");
				kvAdminMessage = new KVAdminMessageImpl();
				kvAdminMessage.setCommand(Commands.MOVE_DATA_FAIL);
			}			

		}else if(kvAdminMessage.getCommand().equals(Commands.REPLICATE)){
			//call moveData_remove()
			logger.info("Executing REPLICATE Command for("+clientSocket.getLocalPort()+")");
			String range = kvAdminMessage.getRange();
			String server = kvAdminMessage.getDestinationAddress();

			String ipPort[] = server.split(":");

			logger.info("Executing REPLICATE Command for("+clientSocket.getLocalPort()+")"
					+", movingData to server="+ipPort[0]+":"+ipPort[1]
							+", rangestart="+new BigInteger(range.split(":")[0],16)
			+", rangeend="+new BigInteger(range.split(":")[1],16));


			Socket socket;
			try {
				socket = new Socket(ipPort[0],Integer.parseInt(ipPort[1]));
				mECServerListener.replicateDataToServer(socket, range);
				kvAdminMessage = new KVAdminMessageImpl();
				kvAdminMessage.setCommand(Commands.REPLICATE_SUCCESS);

			} catch (NumberFormatException e) {
				logger.error("NumberFormatException while Executing REPLICATE Command "+e.getStackTrace().toString());
			} catch (UnknownHostException e) {
				logger.error("UnknownHostException while Executing REPLICATE Command "+e.getStackTrace().toString());
			} catch (IOException e) {
				logger.error("IOException while Executing REPLICATE Command "+e.getStackTrace().toString());
			}

		}else if(kvAdminMessage.getCommand().equals(Commands.PING)){
			logger.info("Executing PING Command for("+clientSocket.getLocalPort()+")");

			kvAdminMessage = new KVAdminMessageImpl();
			kvAdminMessage.setCommand(Commands.ECHO);

		}else{
			logger.info("UNKNOWN command received for("+clientSocket.getLocalPort()+")");
			kvAdminMessage = new KVAdminMessageImpl();
			kvAdminMessage.setCommand(Commands.UNKNOWN);
		}

		return kvAdminMessage;
	}

	public void addKVServerListener(KVServerListener listener) {
		mKVServerListener = listener;
	}
	public void addECServerListener(ECServerListener listener) {
		mECServerListener = listener;
	}

	public void addPerformanceListener(PerformanceListener listener) {
		mPerfListener = listener;
	}
}