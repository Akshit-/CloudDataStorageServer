package ecs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.json.Json;
import javax.json.JsonObjectBuilder;

import org.apache.log4j.Logger;

import common.communication.SocketCommunication;
import common.messages.JSONSerializer;
import common.messages.KVAdminMessage;
import common.messages.KVAdminMessage.Commands;
import common.messages.TextMessage;
import metadata.MetaData;

/**
 * ECS Admin class for managing all topology changes and communicating with KVServer. 
 *
 */
public class ECServer {
	
	List<MetaData> mMetaData;
	List<ServerNodeData> mServerConfig;
	private List<Socket> mEcsClientSockets;

	private HashMap <String,Socket> mEcsClientSocketMap;
	JsonObjectBuilder objectBuilder = Json.createObjectBuilder();
	private HashMap<String, BigInteger> hashMap;
	private TreeMap<String, BigInteger> sorted;
	private FaultDetecter mFaultDetector;
	private SocketCommunication securedSocketCommunication;
	private int mDeadNodeCount = 0;


	private static Logger logger = Logger.getRootLogger();


	public ECServer(String string) {
		securedSocketCommunication = new SocketCommunication();
		File cmdLineConfig = null;
		if(string!=null){
			cmdLineConfig = new File(string);
		} else {
			logger.error("ECServer not created due to invalid name specified for ECS Config file");
		}
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(cmdLineConfig));
		} catch (FileNotFoundException e1) {
			logger.error("ECS Config file not fount at : "+cmdLineConfig.getPath());
			System.out.println("ECS Config file not fount at : "+cmdLineConfig.getPath());
		}
		String line = null;
		String[] tokens = null;
		mServerConfig = new ArrayList<ServerNodeData>();
		try {
			while ((line = reader.readLine()) != null) {
				tokens=line.split(" ");
				if(tokens.length ==3){
					ServerNodeData tempServerConfig = new ServerNodeData(tokens[0],tokens[1],tokens[2],true);
					mServerConfig.add(tempServerConfig);
				} else {
					logger.error("Invalid ECS Config file : "+cmdLineConfig.getPath());
					System.out.println("Invalid ECS Config file : "+cmdLineConfig.getPath());
				}
			}
		} catch (IOException e) {
			logger.error("IOException while reading the ecs config file.");
			System.out.println("IOException while reading the ecs config file.");
		}
	}

	public int getMaxAvailableNodeCount() {
		if(mServerConfig!=null) {
			return mServerConfig.size() - mDeadNodeCount;
		} else {
			logger.debug("getMaxAvailableNodeCount()-->mServerConfig is NULL!!!");
			return 0;
		}
	}

	public int getActivatedNodeCount() {
		if(mMetaData!=null) {
			return mMetaData.size();
		} else {
			logger.debug("getActivatedNodeCount()-->mMetaData is NULL!!!");
			return 0;
		}
	}




	public boolean initService(int numberOfNodes) {
		boolean result = true;
		initMetaData(numberOfNodes);
		mEcsClientSockets = new ArrayList<Socket>();
		mEcsClientSocketMap = new HashMap<String, Socket>();
		for(MetaData metaData: mMetaData ){
			try{
				execSSH(metaData);
				//Creating socket connection to newly started KVServer
				Socket ecsClientSocket = new Socket(metaData.getIP(),Integer.parseInt(metaData.getPort()));
				//Adding socket connection to hashmap and socket list for ease of access
				mEcsClientSocketMap.put(metaData.getIP()+":"+metaData.getPort(), ecsClientSocket);
				mEcsClientSockets.add(ecsClientSocket);

			} catch (IOException e) {
				result = false;
				logger.error("IOException while creating socket connection to new node : "+metaData.getIP()+":"+metaData.getPort());				
				logger.info("initService() + Trying again to create new node : "+metaData.getIP()+":"+metaData.getPort());
				
				try {
					Thread.sleep(4000);
				} catch (InterruptedException e1) {
					logger.error("initService() + Thread InterruptedException");
				}
				//TRY again to create new socket
				Socket ecsClientSocket=null;
				try {
					ecsClientSocket = new Socket(metaData.getIP(),Integer.parseInt(metaData.getPort()));
					
					//Adding socket connection to hashmap and socket list for ease of access
					mEcsClientSocketMap.put(metaData.getIP()+":"+metaData.getPort(), ecsClientSocket);
					mEcsClientSockets.add(ecsClientSocket);
				
				} catch (IOException e1) {
					logger.error("IOException occured again while creating socket connection to new node : "+metaData.getIP()+":"+metaData.getPort());
				}
				
				
				
			}
		}
		logger.info("New KVServers count = "+mEcsClientSockets.size());
		try {
			//Waiting for KVServer to create Client connection and initialize it
			Thread.sleep(1000);
			result = sendKVServersMetaData();
		} catch (InterruptedException e) {
			logger.error("InterruptedException while sleeping before sending metadat to all KV servers.");
			result =false;
		}

		return result;
	}

	public boolean start(){
		boolean result = true;
		for(Socket socket: mEcsClientSockets){
			try {
				TextMessage txtMsg = JSONSerializer.marshalKVAdminMsg(null, Commands.START, "","");
				securedSocketCommunication.sendMessage(socket, txtMsg);
				TextMessage responseTxtMsg = securedSocketCommunication.receiveMessage(socket);
				KVAdminMessage responseKVAdminMsg = JSONSerializer.unmarshalKVAdminMsgForCommand(responseTxtMsg);
				logger.debug("start()-->response from KVServer:"
						+socket.getInetAddress().getHostAddress()
						+":"+socket.getPort()
						+" is "+responseKVAdminMsg.getCommand().toString());

				if(!responseKVAdminMsg.getCommand().equals(Commands.START_SUCCESS)) {
					result = false;
					break;
				} 

			} catch (IOException e) {
				logger.error("IOException while sending start command to KVServer:"
						+socket.getInetAddress().getHostAddress()
						+":"+socket.getPort());
				result =  false;
			} 
		}

		startFaultDetector();
		return result;
	}

	private boolean sendStartCommand(String ip, int port) {
		Socket socket = mEcsClientSocketMap.get(ip+":"+Integer.toString(port));
		try {

			TextMessage txtMsg = JSONSerializer.marshalKVAdminMsg(null, Commands.START, "","");
			securedSocketCommunication.sendMessage(socket, txtMsg);
			TextMessage responseTxtMsg = securedSocketCommunication.receiveMessage(socket);
			KVAdminMessage responseKVAdminMsg = JSONSerializer.unmarshalKVAdminMsgForCommand(responseTxtMsg);
			logger.debug("start(ip,port)-->response from KVServer:"
					+socket.getInetAddress().getHostAddress()
					+":"+socket.getPort()
					+" is "+responseKVAdminMsg.getCommand().toString());

			if(responseKVAdminMsg.getCommand().equals(Commands.START_SUCCESS)) {
				return true;
			} else {
				return false;
			}

		} catch (IOException e) {
			logger.error("IOException while sending start command to KVServer:"
					+socket.getInetAddress().getHostAddress()
					+":"+socket.getPort());
			return false;
		}
	}



	public boolean stop(){
		
		stopFaultDetector(); //stop detector thread
		
		boolean result = true;
		for(Socket socket: mEcsClientSockets){
			try {
				TextMessage txtMsg = JSONSerializer.marshalKVAdminMsg(null, Commands.STOP, "","");
				securedSocketCommunication.sendMessage(socket, txtMsg);
				TextMessage responseTxtMsg = securedSocketCommunication.receiveMessage(socket);
				KVAdminMessage responseKVAdminMsg = JSONSerializer.unmarshalKVAdminMsgForCommand(responseTxtMsg);
				logger.debug("stop()-->response from KVServer:"
						+socket.getInetAddress().getHostAddress()
						+":"+socket.getPort()
						+" is "+responseKVAdminMsg.getCommand().toString());

				if(!responseKVAdminMsg.getCommand().equals(Commands.STOP_SUCCESS)) {
					result = false;
					break;
				} 

			} catch (IOException e) {
				logger.error("IOException while sending stop command to KVServer:"
						+socket.getInetAddress().getHostAddress()
						+":"+socket.getPort());
				result = false;
			}
		}	
		return result;
	}

	public boolean shutDown(){
		boolean result = true;
		
		stopFaultDetector(); //stop detector thread
		
		for(Socket socket: mEcsClientSockets){
			try {
				TextMessage txtMsg = JSONSerializer.marshalKVAdminMsg(null, Commands.SHUTDOWN, "","");
				securedSocketCommunication.sendMessage(socket, txtMsg);
			} catch (IOException e) {
				logger.error("IOException while Shutting down KVServer:"
						+ socket.getInetAddress().getHostAddress()
						+ ":" + socket.getPort());
				result = false;
			} 
		}

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			logger.error("InterruptedException while sleeping after sending shutdown msg to all KVServers");

		}

		mEcsClientSocketMap.clear();
		mEcsClientSocketMap = null;
		mEcsClientSockets.clear();
		mEcsClientSockets=null;
		mMetaData.clear();
		mMetaData = null;
		
		return result;
	}

	private boolean shutDownOldNode(String ip , int port){
		boolean result = true;
		Socket socket = mEcsClientSocketMap.get(ip+":"+Integer.toString(port));
		try {
			TextMessage txtMsg = JSONSerializer.marshalKVAdminMsg(null, Commands.SHUTDOWN, "","");
			securedSocketCommunication.sendMessage(socket, txtMsg);

		} catch (IOException e) {
			logger.error("IOException while Shutting down KVServer:"
					+ socket.getInetAddress().getHostAddress()
					+ ":" + socket.getPort());
			result=false;
		}
		return result;

	}

	private boolean sendKVServersMetaData(){
		boolean result = true;
		for(Socket socket : mEcsClientSockets) {
			try {
				TextMessage txtMsg = JSONSerializer.marshalKVAdminMsg(mMetaData, Commands.INIT, "","");
				securedSocketCommunication.sendMessage(socket, txtMsg);
				TextMessage responseTxtMsg = securedSocketCommunication.receiveMessage(socket);
				KVAdminMessage responseKVAdminMsg = JSONSerializer.unmarshalKVAdminMsgForCommand(responseTxtMsg);
				logger.debug("sendKVServersMetaData()-->response from KVServer:"
						+socket.getInetAddress().getHostAddress()
						+":"+socket.getPort()
						+" is "+responseKVAdminMsg.getCommand().toString());

				if(!responseKVAdminMsg.getCommand().equals(Commands.INIT_SUCCESS)) {
					result = false;
					break;
				}

			} catch (IOException e) {
				logger.error("IOException while sending KVServer metaData init commmand to KVServer:"
						+ socket.getInetAddress().getHostAddress()
						+ ":" + socket.getPort());
				result = false;
			}

		}
		return result;
	}

	private boolean updateKVServersMetaData(){
		boolean result = true;
		for(Socket socket : mEcsClientSockets) {
			try {
				TextMessage txtMsg = JSONSerializer.marshalKVAdminMsg(mMetaData, Commands.UPDATE, "","");
				securedSocketCommunication.sendMessage(socket, txtMsg);
				TextMessage responseTxtMsg = securedSocketCommunication.receiveMessage(socket);
				KVAdminMessage responseKVAdminMsg = JSONSerializer.unmarshalKVAdminMsgForCommand(responseTxtMsg);
				logger.debug("updateKVServersMetaData()-->response from KVServer:"
						+socket.getInetAddress().getHostAddress()
						+":"+socket.getPort()
						+" is "+responseKVAdminMsg.getCommand().toString());
				if(!responseKVAdminMsg.getCommand().equals(Commands.UPDATE_SUCCESS)) {
					result = false;
					break;
				}

			} catch (IOException e) {
				logger.error("updateKVServersMetaData() + IOException while updating KVServers MetaData");
				result = false;
			}

		}
		return result;
	}

	private boolean updateKVServerMetaData(String ip, int port) {

		Socket socket = mEcsClientSocketMap.get(ip+":"+Integer.toString(port));
		try {
			TextMessage txtMsg = JSONSerializer.marshalKVAdminMsg(mMetaData, Commands.UPDATE, "","");
			securedSocketCommunication.sendMessage(socket, txtMsg);
			TextMessage responseTxtMsg = securedSocketCommunication.receiveMessage(socket);
			KVAdminMessage responseKVAdminMsg = JSONSerializer.unmarshalKVAdminMsgForCommand(responseTxtMsg);
			logger.debug("updateKVServerMetaData(ip,port)-->response from KVServer:"
					+socket.getInetAddress().getHostAddress()
					+":"+socket.getPort()
					+" is "+responseKVAdminMsg.getCommand().toString());

			if(responseKVAdminMsg.getCommand().equals(Commands.UPDATE_SUCCESS)) {
				return true;
			} else {
				return false;
			}

		} catch (IOException e) {
			logger.error("updateKVServerMetaData() + IOException while updating KVServer MetaData");
			return false;
		}

	}

	private boolean initNewNodeMetaData(String ip, int port) {
		Socket socket = mEcsClientSocketMap.get(ip+":"+Integer.toString(port));
		try {
			TextMessage txtMsg = JSONSerializer.marshalKVAdminMsg(mMetaData, Commands.INIT, "","");
			securedSocketCommunication.sendMessage(socket, txtMsg);
			TextMessage responseTxtMsg = securedSocketCommunication.receiveMessage(socket);
			KVAdminMessage responseKVAdminMsg = JSONSerializer.unmarshalKVAdminMsgForCommand(responseTxtMsg);
			logger.debug("initNewNodeMetaData()-->response from KVServer:"
					+socket.getInetAddress().getHostAddress()
					+":"+socket.getPort()
					+" is "+responseKVAdminMsg.getCommand().toString());

			if(responseKVAdminMsg.getCommand().equals(Commands.INIT_SUCCESS)) {
				return true;
			} else {
				return false;
			}
		} catch (IOException e) {
			logger.error("initNewNodeMetaData() + IOException while initializing Newly added Node MetaData");
			return false;
		}
	}

	private boolean setLockWrite(String ip, int port) {
		Socket socket = mEcsClientSocketMap.get(ip+":"+Integer.toString(port));
		try {
			TextMessage txtMsg = JSONSerializer.marshalKVAdminMsg(null, Commands.LOCK_WRITE, "","");
			securedSocketCommunication.sendMessage(socket, txtMsg);
			TextMessage responseTxtMsg = securedSocketCommunication.receiveMessage(socket);
			KVAdminMessage responseKVAdminMsg = JSONSerializer.unmarshalKVAdminMsgForCommand(responseTxtMsg);
			logger.debug("setLockWrite()-->response from KVServer:"
					+socket.getInetAddress().getHostAddress()
					+":"+socket.getPort()
					+" is "+responseKVAdminMsg.getCommand().toString());
			if(responseKVAdminMsg.getCommand().equals(Commands.LOCK_WRITE_SUCCESS)) {
				return true;
			} else {
				return false;
			}

		} catch (IOException e) {
			logger.error("setLockWrite() + IOException while setting lockWrite on a Node-"+ip+":"+port);
			return false;
		}

	}

	private boolean releaseLockWrite(String ip, int port) {
		Socket socket = mEcsClientSocketMap.get(ip+":"+Integer.toString(port));
		try {
			TextMessage txtMsg = JSONSerializer.marshalKVAdminMsg(null, Commands.UNLOCK_WRITE, "","");
			securedSocketCommunication.sendMessage(socket, txtMsg);
			TextMessage responseTxtMsg = securedSocketCommunication.receiveMessage(socket);		
			KVAdminMessage responseKVAdminMsg = JSONSerializer.unmarshalKVAdminMsgForCommand(responseTxtMsg);
			logger.debug("releaseLockWrite()-->response from KVServer:"
					+socket.getInetAddress().getHostAddress()
					+":"+socket.getPort()
					+" is "+responseKVAdminMsg.getCommand().toString());

			if(responseKVAdminMsg.getCommand().equals(Commands.UNLOCK_WRITE_SUCCESS)) {
				return true;
			} else {
				return false;
			}

		} catch (IOException e) {
			logger.error("releaseLockWrite() + IOException while releasing lockWrite on a Node-"+ip+":"+port);
			return false;
		}

	}

	private boolean moveDataForAdd(MetaData sourceMetaData, MetaData destMetaData, boolean replicationProcess) {
		Socket socket = mEcsClientSocketMap.get(sourceMetaData.getIP()+":"+sourceMetaData.getPort());
		try {
			TextMessage txtMsg;
			if(!replicationProcess){
				txtMsg = JSONSerializer.marshalKVAdminMsg(null, Commands.MOVE_DATA, 
						destMetaData.getIP()+":"+destMetaData.getPort(),
						destMetaData.getRangeStart()+":"+destMetaData.getRangeEnd());
			}else{
				txtMsg = JSONSerializer.marshalKVAdminMsg(null, Commands.MOVE_DATA_REPLICATE, 
						destMetaData.getIP()+":"+destMetaData.getPort(),
						destMetaData.getRangeStart()+":"+destMetaData.getRangeEnd());
			}
			securedSocketCommunication.sendMessage(socket, txtMsg);
			TextMessage responseTxtMsg = securedSocketCommunication.receiveMessage(socket);		
			KVAdminMessage responseKVAdminMsg = JSONSerializer.unmarshalKVAdminMsgForCommand(responseTxtMsg);

			logger.debug("moveDataForAdd()-->response from KVServer:"
					+socket.getInetAddress().getHostAddress()
					+":"+socket.getPort()
					+" is "+responseKVAdminMsg.getCommand().toString());

			if(responseKVAdminMsg.getCommand().equals(Commands.MOVE_DATA_SUCCESS)) {
				return true;
			} else {
				logger.error("Received response  !MOVE_DATA_SUCCESSS from KVServer-"+socket.getInetAddress().getHostAddress()+":"+socket.getPort());
				return false;
			}

		} catch (IOException e) {
			logger.error("moveDataForAdd() + IOException while moving Data during addNode() process");
			return false;
		}		
	}

	private boolean moveDataForRemove(MetaData sourceMetaData, MetaData destMetaData) {

		Socket socket = mEcsClientSocketMap.get(sourceMetaData.getIP()+":"+sourceMetaData.getPort());
		try {
			TextMessage txtMsg = JSONSerializer.marshalKVAdminMsg(null, Commands.MOVE_DATA, 
					destMetaData.getIP()+":"+destMetaData.getPort(),
					sourceMetaData.getRangeStart()+":"+sourceMetaData.getRangeEnd());

			securedSocketCommunication.sendMessage(socket, txtMsg);
			TextMessage responseTxtMsg = securedSocketCommunication.receiveMessage(socket);		
			KVAdminMessage responseKVAdminMsg = JSONSerializer.unmarshalKVAdminMsgForCommand(responseTxtMsg);

			logger.debug("moveDataForRemove()-->response from KVServer:"
					+socket.getInetAddress().getHostAddress()
					+":"+socket.getPort()
					+" is "+responseKVAdminMsg.getCommand().toString());

			if(responseKVAdminMsg.getCommand().equals(Commands.MOVE_DATA_SUCCESS)) {
				return true;
			} else {
				logger.error("Received response  !MOVE_DATA_SUCCESSS from KVServer-"+socket.getInetAddress().getHostAddress()+":"+socket.getPort());
				return false;
			}

		} catch (IOException e) {
			logger.error("moveDataForRemove() + IOException while moving Data during removeNode() process");
			return false;
		}		
	}


	private boolean replicateData(MetaData successorNodeMetaData,
			MetaData destMetaData, String rangeStart, String rangeEnd) {
		Socket socket = mEcsClientSocketMap.get(successorNodeMetaData.getIP()+":"+successorNodeMetaData.getPort());
		try {
			TextMessage txtMsg = JSONSerializer.marshalKVAdminMsg(null, Commands.REPLICATE, 
					destMetaData.getIP()+":"+destMetaData.getPort(),
					rangeStart+":"+rangeEnd);
			logger.debug("replicateData() replicateData Request from KVServer:"
					+socket.getInetAddress().getHostAddress()
					+":"+socket.getPort()
					+", to server="+destMetaData.getIP()+":"+destMetaData.getPort()
					+", for ranges="+rangeStart+":"+rangeEnd);
			
			securedSocketCommunication.sendMessage(socket, txtMsg);
			TextMessage responseTxtMsg = securedSocketCommunication.receiveMessage(socket);		
			KVAdminMessage responseKVAdminMsg = JSONSerializer.unmarshalKVAdminMsgForCommand(responseTxtMsg);
			logger.debug("moveDataForRemove()-->response from KVServer:"
					+socket.getInetAddress().getHostAddress()
					+":"+socket.getPort()
					+" is "+responseKVAdminMsg.getCommand().toString());
			if(responseKVAdminMsg.getCommand().equals(Commands.REPLICATE_SUCCESS)) {
				logger.debug("replicateData  REPLICATE_SUCCESS from KVServer-"+socket.getInetAddress().getHostAddress()+":"+socket.getPort());
				return true;
			} else {
				logger.error("replicateData !REPLICATE_SUCCESS from KVServer-"+socket.getInetAddress().getHostAddress()+":"+socket.getPort());
				return false;
			}
		} catch (IOException e) {
			logger.error("replicateData() + IOException while replicating Data during "
					+ "for Request from KVServer:"
					+socket.getInetAddress().getHostAddress()
					+":"+socket.getPort()
					+", to server="+destMetaData.getIP()+":"+destMetaData.getPort()
					+", for ranges="+rangeStart+":"+rangeEnd);
			
			return false;
		}	
	}

	public boolean addNode(){

		stopFaultDetector(); //stop detector thread
		
		List<MetaData> oldMetaData = mMetaData;
		MetaData newNodeMetaData = null;
		MetaData successorNodeMetaData = null;
		initMetaData(getActivatedNodeCount()+1);
		int i=0;

		logger.debug("Old MetaData = "+oldMetaData);
		logger.debug("New MetaData = "+mMetaData);

		for(MetaData metaData: oldMetaData){			
			if(!metaData.getIP().equals(mMetaData.get(i).getIP()) || !metaData.getPort().equals(mMetaData.get(i).getPort())){
				//This is the newly added node
				newNodeMetaData = mMetaData.get(i);
				successorNodeMetaData = mMetaData.get(i+1);
				break;
			}
			i++;
		}
		if(newNodeMetaData ==null){
			newNodeMetaData = mMetaData.get(mMetaData.size()-1);
			successorNodeMetaData = mMetaData.get(0);

		}
		try {
			execSSH(newNodeMetaData);
			logger.debug("Adding new socket for newly added server-"+newNodeMetaData.getIP()+":"+newNodeMetaData.getPort());
			Socket ecsClientSocket = new Socket(newNodeMetaData.getIP(),Integer.parseInt(newNodeMetaData.getPort()));
			mEcsClientSocketMap.put(newNodeMetaData.getIP()+":"+newNodeMetaData.getPort(), ecsClientSocket);
			mEcsClientSockets.add(ecsClientSocket);
		} catch (IOException e) {
			logger.error("IOException while creating socket connection to newly added node."+Arrays.toString(e.getStackTrace()));
			logger.info("initService() + Trying again to create new node : "+newNodeMetaData.getIP()+":"+newNodeMetaData.getPort());
			
			try {
				Thread.sleep(4000);
			} catch (InterruptedException e1) {
				logger.error(" Thread InterruptedException while creating socket connection to newly added node."+Arrays.toString(e.getStackTrace()));

			}
			//TRY again to create new socket
			Socket ecsClientSocket=null;
			try {
				ecsClientSocket = new Socket(newNodeMetaData.getIP(),Integer.parseInt(newNodeMetaData.getPort()));
				//Adding socket connection to hashmap and socket list for ease of access
				mEcsClientSocketMap.put(newNodeMetaData.getIP()+":"+newNodeMetaData.getPort(), ecsClientSocket);
				mEcsClientSockets.add(ecsClientSocket);
			
			} catch (IOException e1) {
				logger.error("IOException occured again while creating socket connection to new node : "+newNodeMetaData.getIP()+":"+newNodeMetaData.getPort());
			}
			
		}
		boolean replicationProcess=false;
		if(mMetaData.size()>2){
			replicationProcess = true;
		}
		
		boolean result=false;
		result = initNewNodeMetaData(newNodeMetaData.getIP(), Integer.parseInt(newNodeMetaData.getPort()));
		if(result) {
			result = sendStartCommand(newNodeMetaData.getIP(), Integer.parseInt(newNodeMetaData.getPort()));
			if(result) {
				result = setLockWrite(successorNodeMetaData.getIP(), Integer.parseInt(successorNodeMetaData.getPort()));
				if(result) {
					if(replicationProcess)
						result = moveDataForAdd(successorNodeMetaData, newNodeMetaData,true);
					else
						result = moveDataForAdd(successorNodeMetaData, newNodeMetaData,false);
					if(result) {
						result = updateKVServersMetaData();
						if(result) {
							result = releaseLockWrite(successorNodeMetaData.getIP(), Integer.parseInt(successorNodeMetaData.getPort()));
						}
					}
				}
			}

		}
		startFaultDetector();
		return result;

	}


	public boolean removeNode(){
		
		stopFaultDetector(); //stop detector thread
		
		List<MetaData> oldMetaData = mMetaData;
		MetaData oldNodeMetaData = null;
		MetaData successorNodeMetaData = null;
		boolean result=false;
		if(oldMetaData.size()>1) {
			initMetaData(oldMetaData.size()-1);
			int i=0;
			for(MetaData metaData: mMetaData){
				if(!metaData.getIP().equals(oldMetaData.get(i).getIP()) || !metaData.getPort().equals(oldMetaData.get(i).getPort())){
					//This is the old removed node
					oldNodeMetaData = oldMetaData.get(i);
					successorNodeMetaData = metaData;
					break;
				}
				i++;
			}
			if(oldNodeMetaData==null){
				oldNodeMetaData = oldMetaData.get(oldMetaData.size()-1);
				successorNodeMetaData = oldMetaData.get(0);
			}

			logger.info("removeNode() + oldNode="+oldNodeMetaData.getPort()
					+", start="+oldNodeMetaData.getRangeStart()
					+", end="+oldNodeMetaData.getRangeEnd());

			//Calculating position of successor node
			MetaData targetNode=null;
			i=0;
			boolean replicaCase = false;
			if(mMetaData.size()>2){
				for(MetaData metaData: mMetaData) {
					if(metaData.equals(successorNodeMetaData)){
						break;
					}
					i++;
				}
				if(i+2<=mMetaData.size()-1) {
					targetNode = mMetaData.get(i+2);
				} else if (i+1 == mMetaData.size()-1) {
					targetNode = mMetaData.get(0);
				} else {
					targetNode = mMetaData.get(1);
				}
				replicaCase = true;
			}
			
			if(replicaCase) {
				
				result = replicateData(oldNodeMetaData, targetNode,oldNodeMetaData.getRangeStart(),oldNodeMetaData.getRangeEnd());
				
			}else{
				result = setLockWrite(oldNodeMetaData.getIP(), Integer.parseInt(oldNodeMetaData.getPort()));
				if(result) {
					result = updateKVServerMetaData(successorNodeMetaData.getIP(), Integer.parseInt(successorNodeMetaData.getPort()));
					if(result) {
						result = moveDataForRemove(oldNodeMetaData, successorNodeMetaData);
						if(result) {
							result = releaseLockWrite(successorNodeMetaData.getIP(), Integer.parseInt(successorNodeMetaData.getPort()));
						}
					}

				}
			}
			if(result) {
				result = shutDownOldNode(oldNodeMetaData.getIP(), Integer.parseInt(oldNodeMetaData.getPort()));
			} if(result) {
				mEcsClientSockets.remove(mEcsClientSocketMap.get(oldNodeMetaData.getIP()+":"+oldNodeMetaData.getPort()));
				mEcsClientSocketMap.remove(oldNodeMetaData.getIP()+":"+oldNodeMetaData.getPort());
				result = updateKVServersMetaData();	
			}
			
		} else {
			result = shutDown();
		}
		
		if(getActivatedNodeCount()>0) 
			startFaultDetector();
		
		
		return result;
	}


	private String getMD5(String msg){
		MessageDigest messageDigest = null;
		try {
			messageDigest = MessageDigest.getInstance("MD5");
		} catch(NoSuchAlgorithmException ex){
			logger.error("Storage::getMD5() + Error while computing MD5 for msg.");
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

	private void initMetaData(int nodeCount){
		
		mMetaData = new ArrayList<MetaData>();
		MetaData tempMetaData = null;
		hashMap = new HashMap<String, BigInteger>();
		
		for(int i=0;i<mServerConfig.size() && hashMap.size() < nodeCount; i++){
			
			if( mServerConfig.get(i).isAlive()) {
				String ipPort = mServerConfig.get(i).getIPAddress()+":"+mServerConfig.get(i).getPort();
				BigInteger prevRangeBi = new BigInteger(getMD5(ipPort),16);
				hashMap.put(ipPort, prevRangeBi);
			}
		}
		
		//Sort the meta data so that we can find adjacent node based on their hash values
		ValueComparator vc = new ValueComparator(hashMap);
		sorted = new TreeMap<String, BigInteger>(vc);
		sorted.putAll(hashMap);

		int i = 1;
		MetaData previous = new MetaData();
		for (String key : sorted.keySet()) {
			tempMetaData = new MetaData();
			String tokens[] = key.split(":");
			tempMetaData.setIP(tokens[0]);
			tempMetaData.setPort(tokens[1]);
			tempMetaData.setRangeEnd(sorted.get(key).toString(16));
			if(i>1){
				BigInteger prevRangeBi = new BigInteger(previous.getRangeEnd(),16);
				BigInteger nextRangeBi = prevRangeBi.add(new BigInteger("1"));
				tempMetaData.setRangeStart(nextRangeBi.toString(16));
			}
			mMetaData.add(tempMetaData);
			previous = tempMetaData;
			i++;

		}
		BigInteger prevRangeBi = new BigInteger(mMetaData.get(mMetaData.size()-1).getRangeEnd(),16);
		BigInteger nextRangeBi = prevRangeBi.add(new BigInteger("1"));
		mMetaData.get(0).setRangeStart(nextRangeBi.toString(16));
		for(MetaData md:mMetaData){
			logger.debug("New MetaData = "+md.getPort()
					+", start="+new BigInteger(md.getRangeStart(),16)
			+", end="+new BigInteger(md.getRangeEnd(),16));
		}
	}

	private void execSSH(MetaData metaData) throws IOException{
		String cmd = "java -jar ms3-server.jar "+metaData.getPort();
		//String cmd = "ssh -n "+metaData.getIP()+" nohup java -jar"+" ms3-server.jar "+metaData.getPort()+" ERROR &";
		Runtime run = Runtime.getRuntime();
		run.exec(cmd);
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			logger.error("InterruptedException after execSSH command for KVServer:"+metaData.getIP()+":"+metaData.getPort());
		}
	}

	//This class is used to sort the metadata using end-range value
	static class ValueComparator implements Comparator<String> {
		Map<String, BigInteger> base;
		ValueComparator(Map<String, BigInteger> base) {
			this.base = base;
		}

		@Override
		public int compare(String a, String b) {
			BigInteger x = base.get(a);
			BigInteger y = base.get(b);
			return x.compareTo(y);
		}
	}

	public List<Socket> getECSServerSockets() {
		return mEcsClientSockets;
	}
	
	

	private void startFaultDetector(){	
		mFaultDetector = new FaultDetecter(this);
		mFaultDetector.start();
	}

	private void stopFaultDetector(){	
		
		if(mFaultDetector!=null){
			mFaultDetector.setStop(true);
			mFaultDetector = null;
		}
		
	}
	
	public void handleFaultyServer(Socket socket) {
		
		mDeadNodeCount++;
		
		stopFaultDetector(); //stop detector thread
		
		logger.debug("Faulty socket::"+socket.getInetAddress().getHostAddress()+":"+socket.getPort());
		for(int i=0;i<mServerConfig.size();i++) {
			if(socket.getPort() == Integer.parseInt(mServerConfig.get(i).getPort())) {
				mServerConfig.get(i).setAlive(false);
				mEcsClientSockets.remove(mEcsClientSocketMap.get(mServerConfig.get(i).getIPAddress()+":"+mServerConfig.get(i).getPort()));
				mEcsClientSocketMap.remove(mServerConfig.get(i).getIPAddress()+":"+mServerConfig.get(i).getPort());
			}
		}
		
		logger.debug("Old metadata\n");
		for(MetaData md:mMetaData){
			logger.debug("MetaData = "+md.getPort()
					+", start="+new BigInteger(md.getRangeStart(),16)
			+", end="+new BigInteger(md.getRangeEnd(),16));
		}
		
		initMetaData(getActivatedNodeCount()-1);
		
		logger.debug("New metadata\n");
		for(MetaData md:mMetaData){
			logger.debug("MetaData = "+md.getPort()
					+", start="+new BigInteger(md.getRangeStart(),16)
			+", end="+new BigInteger(md.getRangeEnd(),16));
		}
		
		boolean result = updateKVServersMetaData();	
		
		if(result){
			logger.debug("Recovered from Fault Succesfully");
			startFaultDetector();
		} else {
			logger.debug("Dooms day... Fault could not be recovered");
		}
	}
	

}
