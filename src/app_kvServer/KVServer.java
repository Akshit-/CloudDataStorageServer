package app_kvServer;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import logger.LogSetup;
import metadata.MetaData;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import server.ClientConnection;
import server.ECServerListener;
import server.KVServerListener;
import server.PerformanceListener;
import server.Server;
import server.storage.Storage;
import common.communication.SocketCommunication;
import common.messages.JSONSerializer;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.TextMessage;

/**
 * KVServer class for handling client's request by retrieving key-value pairs
 * from Storage class. And is also responsible for handling ECServer messages 
 * by managing data stored in Storage. 
 *
 */
public class KVServer extends Thread implements KVServerListener, ECServerListener, PerformanceListener{

	private static Logger logger = Logger.getRootLogger();

	private static int port;

	private ServerSocket serverSocket;
	private boolean running;

	private Storage storage;
	private Socket mReplica1Socket;
	private Socket mReplica2Socket;
	private Server mServerData;

	//used for Performance testing
	private static final long TIME_INTERV = 10000;
	private long time;
	private int totalbytes;
	private long totaltime;
	private int throughput;
	private int sec = 0 ;
	private String statistics = "";

	private SocketCommunication securedsSocketCommunication;

	/**
	 * Constructs a KV Server object which listens to connection attempts 
	 * at the given port.
	 * 
	 * @param port a port number which the Server is listening to in order to 
	 * 		establish a socket connection to a client. The port number should 
	 * 		reside in the range of dynamic ports, i.e 49152 – 65535.
	 */
	public KVServer(int port) {
		KVServer.port = port;
		storage = Storage.init();
		mServerData = Server.getInstance();
		securedsSocketCommunication = new SocketCommunication();
	}

	/**
	 * Initializes and starts the server. 
	 * Loops until the the server should be closed.
	 */
	@Override
	public void run() {
		running = initializeServer();
		if(serverSocket != null) {
			while(isRunning()){
				try {
					Socket client = serverSocket.accept();
					logger.info("KVServer:"+serverSocket.getInetAddress().getHostAddress()+":"+serverSocket.getLocalPort()+"Connected to " 
							+ client.getInetAddress().getHostName() 
							+  " on port " + client.getLocalPort());
					ClientConnection connection = 
							new ClientConnection(client);

					connection.addKVServerListener(this);
					connection.addECServerListener(this);
					connection.addPerformanceListener(this);
					new Thread(connection).start();

				} catch (IOException e) {
					logger.error("Error! " +
							"Unable to establish connection. \n", e);
				}
			}
		}
		logger.info("Server stopped.");
	}
	
	private boolean isRunning() {
		return this.running;
	}

	@Override
	public boolean isActiveForClients() {
		return mServerData.isActiveForClients();
	}

	@Override
	public boolean isLockWrite() {
		return mServerData.isLockWrite();
	}

	/**
	 * Stops the server insofar that it won't listen at the given port any more.
	 */
	public void stopServer(){
		running = false;
		try {
			serverSocket.close();
			logger.info("stopServer()-->Successfully stopped the KVServer");
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + port, e);
		}
	}
	
	/**
	 * Initializes server by creating server socket.
	 * @return
	 * 		true if successfully initialized else false.
	 */
	private boolean initializeServer() {
		logger.info("Initialize server ...");
		try {
			serverSocket = new ServerSocket(port);
			System.out.println("Server listening on port: " 
					+ serverSocket.getLocalPort());
			logger.info("Server listening on port: " 
					+ serverSocket.getLocalPort());    
			return true;

		} catch (IOException e) {
			logger.error("Error! Cannot open server socket:");
			if(e instanceof BindException){
				logger.error("Port " + port + " is already bound!");
			}
			return false;
		}
	}

	/**
	 * Method to delete Moved Data from the KVServer storage.
	 * @param movedData
	 * 		Data to be removed from this server.
	 * @return
	 * 		Returns true if delete operation was successful else false.
	 */
	private boolean deleteMovedData(HashMap<String, String> movedData) {
		return storage.deleteDataBetweenRange(movedData);
	}

	@Override
	public void initKVServer(List<MetaData> metaDatas){
		
		//store service metadata
		mServerData.setServiceMetaData(new ArrayList<MetaData>(metaDatas));
		List<MetaData> mMetaDatas = mServerData.getServiceMetaData();
		logger.info("initKVServer()");
		int i=0;
		for(MetaData meta : mMetaDatas){
			if(meta.getPort().equals(Integer.toString(port))){
				// store node metadata
				mServerData.setNodeMetaData(meta);
				break;
			}
			i++;
		}

		if(mMetaDatas.size()>2){
			try {
				// Setting up meta data for nodes which will act as REPLICA'S for this server
				if(i+2  <=  mMetaDatas.size()-1) {
					//Generic Case: here new Node is somewhere between
					logger.info("initKVServer()--setMyReplicaMetaData--1");
					mServerData.setMyReplica1MetaData(mMetaDatas.get(i+1));
					mServerData.setMyReplica2MetaData(mMetaDatas.get(i+2));
				} else if (i+1 == mMetaDatas.size()-1) {
					//here new Node is second last
					logger.info("initKVServer()--setMyReplicaMetaData--2");
					mServerData.setMyReplica1MetaData(mMetaDatas.get(i+1));
					mServerData.setMyReplica2MetaData(mMetaDatas.get(0));
				} else {
					//here new Node is last
					logger.info("initKVServer()--setMyReplicaMetaData--3");
					mServerData.setMyReplica1MetaData(mMetaDatas.get(0));
					mServerData.setMyReplica2MetaData(mMetaDatas.get(1));
				}

				// Setting up meta data for nodes for which this server acts as REPLICA
				if(i-2 >= 0) {
					//Generic Case
					logger.info("initKVServer()--setReplicaMetaData--1");
					mServerData.setReplica1MetaData(mMetaDatas.get(i-1));
					mServerData.setReplica2MetaData(mMetaDatas.get(i-2));
				} else if (i-1 == 0) {
					//Second Node
					logger.info("initKVServer()--setReplicaMetaData--2");
					mServerData.setReplica1MetaData(mMetaDatas.get(i-1));
					mServerData.setReplica2MetaData(mMetaDatas.get(mMetaDatas.size()-1));
				} else {
					//First Node
					logger.info("initKVServer()--setReplicaMetaData--3");
					mServerData.setReplica1MetaData(mMetaDatas.get(mMetaDatas.size()-1));
					mServerData.setReplica2MetaData(mMetaDatas.get(mMetaDatas.size()-2));
				}

				setMyReplica1Socket(new Socket(getMyReplica1MetaData().getIP(),
						Integer.parseInt(getMyReplica1MetaData().getPort())));

				setMyReplica2Socket(new Socket(getMyReplica2MetaData().getIP(),
						Integer.parseInt(getMyReplica2MetaData().getPort())));

			} catch(Exception e){
				logger.error("Exception while setting replica meta data."+e);
			}

			MetaData mMetaData = mServerData.getNodeMetaData();
			try {
				logger.info("Storing MetaData corresponding to this Server: "
						+mMetaData.getIP()
						+":"+mMetaData.getPort()
						+" "+mMetaData.getRangeStart()
						+" "+mMetaData.getRangeEnd());
				logger.info("Storing MetaData corresponding to Replica 1 for this Server: "
						+getMyReplica1MetaData().getIP()
						+":"+getMyReplica1MetaData().getPort()
						+" "+getMyReplica1MetaData().getRangeStart()
						+" "+getMyReplica1MetaData().getRangeEnd());
				logger.info("Storing MetaData corresponding to Replica 1 for this Server: "
						+getMyReplica2MetaData().getIP()
						+":"+getMyReplica2MetaData().getPort()
						+" "+getMyReplica2MetaData().getRangeStart()
						+" "+getMyReplica2MetaData().getRangeEnd());

				logger.info("Storing MetaData corresponding to replica 1 at this server: "
						+getReplica1MetaData().getIP()
						+":"+getReplica1MetaData().getPort()
						+" "+getReplica1MetaData().getRangeStart()
						+" "+getReplica1MetaData().getRangeEnd());
				logger.info("Storing MetaData corresponding to replica 2 at this Server: "
						+getReplica2MetaData().getIP()
						+":"+getReplica2MetaData().getPort()
						+" "+getReplica2MetaData().getRangeStart()
						+" "+getReplica2MetaData().getRangeEnd());
			}catch(Exception e){
				logger.error("Exception while printing replica meta data."+e);
			}
		}
	}

	@Override
	public void startKVServer(){
		mServerData.setIsActiveForClients(true);
	}

	@Override
	public void stopKVServer(){
		mServerData.setIsActiveForClients(false);
	}

	@Override
	public void lockWrite(){
		mServerData.setLockWrite(true);
	}

	@Override
	public void unlockWrite(){
		mServerData.setLockWrite(false);
	}

	@Override
	public MetaData getNodeMetaData() {
		return mServerData.getNodeMetaData();
	}

	@Override
	public MetaData getReplica1MetaData() {
		return mServerData.getReplica1MetaData();
	}

	@Override
	public MetaData getMyReplica1MetaData() {
		return mServerData.getMyReplica1MetaData();
	}

	@Override
	public MetaData getReplica2MetaData() {
		return mServerData.getReplica2MetaData();
	}

	@Override
	public MetaData getMyReplica2MetaData() {
		return mServerData.getMyReplica2MetaData();
	}

	@Override
	public List<MetaData> getServiceMetaData() {
		return mServerData.getServiceMetaData();
	}	

	@Override
	public Socket getMyReplica1Socket(){
		return mReplica1Socket;

	}

	@Override
	public void setMyReplica1Socket(Socket s) {
		mReplica1Socket = s;
	}

	@Override
	public Socket getMyReplica2Socket() {
		return mReplica2Socket;
	}

	@Override
	public void setMyReplica2Socket(Socket s) {
		mReplica2Socket = s;
	}

	@Override
	public boolean moveData(String range, String server, boolean replication) {
		String ipPort[] = server.split(":");
		logger.info("KVServer::moveData() + Starting moveData process to new Server="+ipPort[1]);

		HashMap<String, String> dataToBeMoved = storage.getDataBetweenRange(range);
		if(dataToBeMoved.isEmpty()){
			logger.info("KVServer::moveData() + Nothing to be Moved!");
			return true;
		}

		Socket moveDataServer = null;	
		try {
			moveDataServer = new Socket(ipPort[0], Integer.parseInt(ipPort[1]));

			logger.info("KVServer::moveData() + Socket created for KVServer="+ipPort[1]);
		} catch (IOException e) {
			logger.error("KVServer::moveData() + Error creating socket for New Server ="+e);
			return false;
		}

		for(Iterator<Entry<String, String>>it=dataToBeMoved.entrySet().iterator();it.hasNext();){
			Entry<String, String> entry = it.next();
			TextMessage txtMsg = JSONSerializer.marshal(entry.getKey(), entry.getValue(), 
					StatusType.PUT);
			TextMessage deletefromR2 = JSONSerializer.marshal(entry.getKey(), "", 
					StatusType.REPLICA_PUT);

			logger.debug("KVServer::moveData() + Sending data to KVserver="+entry.getKey()+","+entry.getValue());

			try {				
				securedsSocketCommunication.sendMessage(moveDataServer, txtMsg);			
				//Respone has to be PUT_SUCCESS
				TextMessage responseTxtMsg = securedsSocketCommunication.receiveMessage(moveDataServer);
				KVMessage responseKVMsg = JSONSerializer.unMarshal(responseTxtMsg);

				if(responseKVMsg.getStatus()!=StatusType.PUT_SUCCESS ){
					logger.info("KVServer::moveData() + Couldn't move Data to new Server!");
					continue;
				}

			} catch (IOException e) {
				logger.error("KVServer::moveData() + Error while sending data to new Server : "+e);
				continue;
			}

			//Send message to myR2 to delete these datas
			try {
				if(getServiceMetaData().size()>2 && mReplica2Socket!=null){
					securedsSocketCommunication.sendMessage(mReplica2Socket, deletefromR2);
				}
			} catch (IOException e) {
				logger.error("KVServer::moveData() + Error while sending data to mReplica2Socket: "+mReplica2Socket+"="+e);
				return false;
			}
		}

		//Tell myReplica1 to delete his R2
		if(getServiceMetaData().size()>2 && mReplica1Socket!=null){
			try { 
				TextMessage txtMsg = JSONSerializer.marshal(StatusType.DELETE_TOPOLOGICAL);
				logger.debug("KVServer::moveData() + Sending DELETE_TOPOLOGICAL to mReplica1Socket:"+mReplica1Socket);
				securedsSocketCommunication.sendMessage(mReplica1Socket, txtMsg);
			} catch (IOException e) {
				logger.error("KVServer::moveData() + Error while sending data to mReplica1Socket:"+mReplica1Socket+" = "+e);
				return false;
			}
		}

		//if replicaCondition exists lets not delete from successor, need it for replica!
		if(!replication){
			deleteMovedData(dataToBeMoved);
		}

		logger.info("KVServer::moveData() + Successfully moved Data to New Server!");
		return true;
	}

	@Override
	public boolean deleteDataBetween(MetaData mdata) {
		String range = mdata.getRangeStart()+":" + mdata.getRangeEnd();
		HashMap<String, String> dataToBeDeleted= storage.getDataBetweenRange(range);

		return deleteMovedData(dataToBeDeleted);
	}

	@Override
	public void replicateDataToServer(Socket socket, String range) {
		HashMap<String, String> dataToBeMoved = storage.getDataBetweenRange(range);
		if(dataToBeMoved.isEmpty()){
			logger.info("KVServer::replicateDataToServer() + Nothing to be Moved!");
			return;
		}
		logger.debug("KVServer::replicateDataToServer()-->KVserver="+socket);
		for(Iterator<Entry<String, String>>i=dataToBeMoved.entrySet().iterator(); i.hasNext();){
			Entry<String, String> entry = i.next();

			TextMessage txtMsg = JSONSerializer.marshal(entry.getKey(), entry.getValue(),StatusType.REPLICA_PUT);

			logger.debug("KVServer::replicateDataToServer() + Sending data:="+entry.getKey()+","+entry.getValue());

			try {
				securedsSocketCommunication.sendMessage(socket, txtMsg);
			} catch (IOException e) {
				logger.error("KVServer::replicateDataToServer()-->IOException while sending to socket="+socket);
				continue;//do not try to receiveMessage if you were not able to send it will result in deadlock
			}
			KVMessage kvMsgReply = null;
			try {
				kvMsgReply = JSONSerializer.unMarshal(securedsSocketCommunication.receiveMessage(socket));
				if(kvMsgReply.getStatus().equals(StatusType.REPLICA_PUT_SUCCESS)
						||kvMsgReply.getStatus().equals(StatusType.REPLICA_PUT_UPDATE)){
					//Replication success
					logger.debug("KVServer::replicateDataToServer()-->Success="+entry.getKey()+","+entry.getValue());
				} else {
					logger.error("KVServer::replicateDataToServer()-->Error="+entry.getKey()+","+entry.getValue());
				}
			} catch (IOException e) {
				logger.error("KVServer::replicateDataToServer()-->IOException while receiving from socket="+socket);
			}
		}
	}

	@Override
	public void update(List<MetaData> metadatas) {
		initKVServer(metadatas);
	}

	@Override
	public String put(String key, String value){
		return storage.put(key, value);
	}

	@Override
	public String delete(String key){	
		return storage.delete(key);
	}

	@Override
	public String get(String key){
		return storage.get(key);
	}

	/**
	 * Method to set logLevel for KVServer.
	 * @param levelString
	 * 		Level to be set for logs.
	 * @return
	 * 		Return String representation of the level which was set.
	 */
	private static String setLevel(String levelString) {
		logger.info("setLevel() + setting logLevel for KVServer to "+ levelString);
		if(levelString.equals(Level.ALL.toString())) {
			logger.setLevel(Level.ALL);
			return Level.ALL.toString();
		} else if(levelString.equals(Level.DEBUG.toString())) {
			logger.setLevel(Level.DEBUG);
			return Level.DEBUG.toString();
		} else if(levelString.equals(Level.INFO.toString())) {
			logger.setLevel(Level.INFO);
			return Level.INFO.toString();
		} else if(levelString.equals(Level.WARN.toString())) {
			logger.setLevel(Level.WARN);
			return Level.WARN.toString();
		} else if(levelString.equals(Level.ERROR.toString())) {
			logger.setLevel(Level.ERROR);
			return Level.ERROR.toString();
		} else if(levelString.equals(Level.FATAL.toString())) {
			logger.setLevel(Level.FATAL);
			return Level.FATAL.toString();
		} else if(levelString.equals(Level.OFF.toString())) {
			logger.setLevel(Level.OFF);
			return Level.OFF.toString();
		} else {
			return LogSetup.UNKNOWN_LEVEL;
		}
	}

	/**
	 * Main entry point for KVServer application. 
	 * @param args 
	 * 		contains the port number at args[0] and logLevel at args[1]
	 */
	public static void main(String[] args) {
		try {
			new LogSetup("logs/server/server.log", Level.ALL);
			if(args.length < 1 && args.length>2) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: KVServer <port> <logLevel>!");
				System.out.println("Usage: <logLevel> is optional. Possible levels <ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF>");
				System.exit(1);
			} else {
				if(args.length==1) {
					port = Integer.parseInt(args[0]);
					new KVServer(port).start();
				} else if(LogSetup.isValidLevel(args[1])) {
					port = Integer.parseInt(args[0]);
					setLevel(args[1]); 
					new KVServer(port).start(); 
				} else {
					System.out.println("Error! Invalid logLevel");
					System.out.println("Possible levels <ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF>");
					System.exit(1);
				}


			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: KVServer <port> <logLevel>!");
			System.out.println("Usage: KVServer <port> <logLevel>!");
			System.out.println("Usage: <logLevel> is optional. Possible levels <ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF>");
			System.exit(1);
		}
	}

	@Override
	public void calculateThroughput(int receivedBytes) {


		// measure throughput for a specified time interval of 10 secs
		time = System.currentTimeMillis() - time;

		System.out.println("THROUGHPUT!!!!!!!!!!!!!!!!!!!!!!!!!!");
		totalbytes += receivedBytes; // for each request
		if (time < TIME_INTERV) totaltime += time;
		if (totaltime >= TIME_INTERV) {

			throughput = (totalbytes * 8) / 1024;
			statistics =+ throughput+"\t"+sec+"\n";
			System.out.println(statistics);
			totalbytes = 0;
			time = totaltime = 0;
		}

		sec+=10;
	}

	@Override
	public String displayStatistics() {
		System.out.println(">Throughput - ServerPort:"+KVServer.port);
		return statistics;
	}	

}
