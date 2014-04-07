package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.math.BigInteger;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import metadata.MetaData;

import org.apache.log4j.Logger;

import common.communication.SocketCommunication;
import common.messages.JSONSerializer;
import common.messages.KVMessage;
import common.messages.KVMessageImpl;
import common.messages.TextMessage;
import common.messages.KVMessage.StatusType;

/**
 * KVStore module acts as a program library for client applications 
 * in general and encapsulates the complete functionality to use a KV 
 * storage service running somewhere on the Internet.
 *
 */
public class KVStore extends Thread implements KVCommInterface {
	private Logger logger = Logger.getRootLogger();
	private Set<ClientSocketListener> listeners;
	private boolean running;

	private String mAddress;
	private int mPort;
	private Socket clientSocket;
	private OutputStream output;
	private InputStream input;
	private List<MetaData> metadata;
	private MetaData currentMetaData;
	private boolean firstTime;
	private KVMessage redirected;
	private String statistics = "";
	
	//for sending and receiving socket messages
	private SocketCommunication securedsSocketCommunication;
	/**
	 * Initialize KVStore with address and port of KVServer
	 * 
	 * @param address
	 *            the address of the KVServer
	 * @param port
	 *            the port of the KVServer
	 */
	public KVStore(String address, int port) {
		mAddress = address;
		mPort = port;
		listeners = new HashSet<ClientSocketListener>();
		this.currentMetaData = new MetaData(mAddress, mPort + "", "", "");
		firstTime = true;
		securedsSocketCommunication = new SocketCommunication();
	}

	/**
	 * Tries to establish connection to the server on address and port
	 * initialized in constructor This method must only be called after
	 * initializing instance with {@link Constructor}
	 * 
	 * @throws Exception
	 *             if unable to connect with servver
	 * 
	 */
	@Override
	public void connect() throws Exception {
		clientSocket = new Socket(mAddress, mPort);
		if (clientSocket != null) {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
			setRunning(true);
			logger.info("Connection established");
			// latestMsg = receiveMessage();
			// for (ClientSocketListener listener : listeners) {
			// listener.handleNewMessage(latestMsg);
			// }
		}
	}

	/**
	 * Disconnects from the currently connected server. This method must only be
	 * called after connection has been established.
	 */
	@Override
	public void disconnect() {
		logger.info("try to close connection ...");

		try {
			tearDownConnection();
			/*
			 * for (ClientSocketListener listener : listeners) {
			 * listener.handleStatus(SocketStatus.DISCONNECTED); }
			 */
		} catch (IOException ioe) {
			logger.error("Unable to close connection!");
		}
	}

	/**
	 * Closes the input/output stream to and closes the client socket.
	 * 
	 * @throws IOException
	 */
	private void tearDownConnection() throws IOException {
		setRunning(false);
		logger.info("tearing down the connection ...");
		if (clientSocket != null) {
			if (input != null) {
				input.close();
				input = null;
			}

			if (output != null) {
				output.close();
				output = null;
			}
			clientSocket.close();
			clientSocket = null;
			logger.info("connection closed!");
		}
	}

	/**
	 * This method tells if the client thread is running or not.
	 * 
	 * @return true if thread is running else returns false
	 */
	public synchronized boolean isRunning() {
		return running;
	}

	/**
	 * This method sets client thread is running or not.
	 * 
	 * @param run
	 *            status to be set
	 */
	public synchronized void setRunning(boolean run) {
		running = run;
	}

	/**
	 * This method add listener for client incoming messages.
	 * 
	 * @param listener
	 */
	public void addListener(ClientSocketListener listener) {
		listeners.add(listener);
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {

		if (isRunning()) {
			if (isResponsible(key, value, StatusType.PUT)) {
				long start = 0;
				try {
					if (value!=null && !value.equalsIgnoreCase("null")){

						TextMessage txtMsg = JSONSerializer.marshal(key, value,//error
								StatusType.PUT);
						logger.info("Sending : " + txtMsg.getMsg());
						start = System.nanoTime();
						securedsSocketCommunication.sendMessage(clientSocket,txtMsg);

					} else {
						TextMessage txtMsg = JSONSerializer.marshal(key, "",
								StatusType.PUT);
						logger.info("Sending : " + txtMsg.getMsg());
						start = System.nanoTime();

						securedsSocketCommunication.sendMessage(clientSocket,txtMsg);
					}
					KVMessage kvmsg = processReply(securedsSocketCommunication.receiveMessage(clientSocket), StatusType.PUT);

					long end = System.nanoTime();
					long err = System.nanoTime() - end;
					long time = end - start - err;

					double seconds = time/1000000000.0;

					statistics = statistics +" "+ String.valueOf(seconds);				    

					return kvmsg;
				} catch (IOException ioe) {
					tearDownConnection();
					logger.error("IOException! Unable to put value to KV server");
					throw new Exception("Unable to put value to KV server");
				}
			} else {
				return this.redirected;
			}
		} else {
			logger.error("Not connected to KV Server!");
			throw new Exception("Not connected to KV Server!");
		}
	}

	/**
	 * Function that re-direct a request to the responsible server
	 * 
	 * @param key
	 * @param value
	 * @param reqStatus
	 * @return
	 * @throws Exception 
	 */
	private boolean isResponsible(String key, String value, StatusType reqStatus) throws Exception {
		if (this.currentMetaData.getRangeStart().equals(""))
			return true;// first time

		for (MetaData meta : this.metadata) {
			if (!serverNotResponsible(meta, key)) {
				if (meta.equals(this.currentMetaData))
					return true;
				else {
					logger.info("Client redirect: connecting to "
							+ meta.getIP() + ":"
							+ Integer.parseInt(meta.getPort()));
					KVStore responsibleServerConn = new KVStore(meta.getIP(),
							Integer.parseInt(meta.getPort()));

					/*System.out.print("Client redirect: connecting to "
							+ meta.getIP() + ":"
							+ Integer.parseInt(meta.getPort()));*/
					try {
						responsibleServerConn.connect();
						logger.info("Client redirect: connecting to "
								+ meta.getIP() + ":"
								+ Integer.parseInt(meta.getPort()));
						if (reqStatus.equals(StatusType.PUT)) {
							try {


								redirected = responsibleServerConn.put(key,
										value);

								statistics = statistics + " " + responsibleServerConn.statistics; //evaluation

								logger.info("PUT Key-value pair on KVServer:"+meta.getPort()+", key:value="+key+","+value);

							} catch (Exception e) {
								logger.error("Unable to add Key-value pair on KVServer listening on"
										+ meta.getPort());

								//System.out.println("err " + e);
							}
						} else {
							try {

								redirected = responsibleServerConn.get(key);
								statistics = statistics + " " + responsibleServerConn.statistics; //evaluation
								logger.info("GET value for key="+key+" on KVServer="+redirected.getValue()+" for "+meta.getPort());

							} catch (Exception e) {
								logger.error("Unable to get Key-value pair from KVServer listening on"
										+ meta.getPort());
								//System.out.println("err " + e);
							}
						}

						/*System.out.println("Disconnected from " + meta.getIP()
								+ ":" + Integer.parseInt(meta.getPort()));*/
						responsibleServerConn.disconnect();
						return false;
					} catch (Exception e1) {
						//e1.printStackTrace();
						logger.error("Client unable to connect to "
								+ meta.getIP() + ":"
								+ Integer.parseInt(meta.getPort()));

						KVStore retryDefaultServer = new KVStore("localhost",
								50000);
						try {
							retryDefaultServer.connect();

							if (reqStatus.equals(StatusType.PUT)) {
								redirected = retryDefaultServer.put(key,
										value);
							}else{
								redirected = retryDefaultServer.get(key);
							}

						}catch(Exception e){
							throw new Exception("Unable to put value to KV server");
						}
						//continue;
						//throw new Exception("Unable to put value to KV server");

					}

				}
			}
		}
		return false;
	}

	/**
	 * Processes the servers reply and transparently handles Client's response
	 * to Storage Service
	 * 
	 * @param reply
	 * @return
	 */
	private synchronized KVMessage processReply(TextMessage reply,
			StatusType reqStatus) {
		KVMessageImpl replyMsg = JSONSerializer.unMarshal(reply);
		String key = replyMsg.getKey();
		StatusType status = replyMsg.getStatus();
		logger.info("KVStore:: Server response: " + reply.getMsg()+", status="+status.toString());
		/**
		 * In this case, server sends a message
		 */
		if (status.equals(StatusType.SERVER_NOT_RESPONSIBLE)) {
			// store metadata
			this.metadata = replyMsg.getMetaData();
			if (firstTime) {
				//System.out.print("firsttime:" + firstTime);
				updateCurrentServerRange();
				firstTime = false;
			} else {
				//System.out.print("firsttime:" + firstTime);
			}
			for (MetaData meta : this.metadata) {

				if (!serverNotResponsible(meta, key)) { // handles
					// server_not_responsible
					// message
					logger.info("Client redirect: connecting to "
							+ meta.getIP() + ":"
							+ Integer.parseInt(meta.getPort()));
					KVStore responsibleServerConn = new KVStore(meta.getIP(),
							Integer.parseInt(meta.getPort()));
					try {
						responsibleServerConn.connect();
						logger.info("Client redirect: connecting to "
								+ meta.getIP() + ":"
								+ Integer.parseInt(meta.getPort()));

						/*System.out.print("Client redirect: connecting to "
								+ meta.getIP() + ":"
								+ Integer.parseInt(meta.getPort()));*/

						if (reqStatus.equals(StatusType.PUT)) {
							try {

								replyMsg = (KVMessageImpl) responsibleServerConn
										.put(replyMsg.getKey(),
												replyMsg.getValue());
							} catch (Exception e) {
								logger.error("Unable to add Key-value pair on KVServer listening on"
										+ meta.getPort());
							}
						} else {
							try {
								replyMsg = (KVMessageImpl) responsibleServerConn
										.get(replyMsg.getKey());
							} catch (Exception e) {
								logger.error("Unable to get Key-value pair from KVServer listening on"
										+ meta.getPort());
							}
						}

						/*System.out.println("Disconnected from " + meta.getIP()
								+ ":" + Integer.parseInt(meta.getPort()));*/
						responsibleServerConn.disconnect();
						break;
					} catch (Exception e1) {
						// TODO Auto-generated catch block
						//e1.printStackTrace();
						logger.error("Client unable to connect to "
								+ meta.getIP() + ":"
								+ Integer.parseInt(meta.getPort()));
					}

				}
			}
		} else if (status.equals(StatusType.SERVER_STOPPED)) {
			logger.info("server is stopped, the request was rejected");
			//System.out.print("server is unavailable, the request was rejected");
		} else if (status.equals(StatusType.SERVER_WRITE_LOCK)) {
			//System.out.println("Server locked for put, only get possible");
			logger.info("Server locked for out, only get possible");
		}

		logger.info("Server reply to client query:"+replyMsg.getStatus().toString());
		return replyMsg;

	}

	/**
	 * Updates connected server metadata
	 */
	private void updateCurrentServerRange() {
		for (MetaData meta : this.metadata) {
			if (meta.getPort().equals(mPort + "")) {
				this.currentMetaData = meta;
				break;
			}
		}
	}

	/**
	 * check whether the pair belongs to server's subset if it doesn't belong
	 * return true else return false
	 * 
	 * @param kvmessage
	 * @return true if the server is not in charge of the particular request
	 */
	private boolean serverNotResponsible(MetaData node, String key_) {

		// Corrected Logic

		BigInteger key = new BigInteger(getMD5(key_), 16);

		BigInteger startServer = new BigInteger(node.getRangeStart(), 16);
		BigInteger endServer = new BigInteger(node.getRangeEnd(), 16);

		BigInteger maximum = new BigInteger("ffffffffffffffffffffffffffffffff",
				16);

		BigInteger minimum = new BigInteger("00000000000000000000000000000000",
				16);

		logger.info("ClientConnection::serverNotResponsible() + key=" + key
				+ ", Server's start=" + startServer + ", Server's end="
				+ endServer + ", Maximum =" + maximum + ", Minimum =" + minimum);

		if (startServer.compareTo(endServer) < 0) {
			if (key.compareTo(startServer) > 0 && key.compareTo(endServer) <= 0) {

				logger.info("ClientConnection::serverNotResponsible(start<end) + return false");
				return false;
			}
		} else {
			// startServer > endServer
			// keycheck1 = startServer to Maximum && keycheck2 = 0 to end
			if ((key.compareTo(startServer) > 0 && key.compareTo(maximum) <= 0)
					|| (key.compareTo(minimum) >= 0 && key.compareTo(endServer) <= 0)) {

				logger.info("ClientConnection::serverNotResponsible(start > end) + return false");
				return false;
			}

		}
		logger.info("ClientConnection::serverNotResponsible() + return true");
		return true;
	}

	private String getMD5(String msg){
		MessageDigest messageDigest = null;
		try {
			messageDigest = MessageDigest.getInstance("MD5");
		} catch(NoSuchAlgorithmException ex){
			logger.debug("not able to cypher key");
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
	@Override
	public KVMessage get(String key) throws Exception {
		if (isRunning()) {

			if (isResponsible(key, "", StatusType.GET)) {
				try {

					TextMessage txtMsg = JSONSerializer.marshal(key, "",
							StatusType.GET);
					logger.info("Sending(GET) : " + txtMsg.getMsg());
					
					long start = System.nanoTime();
					
					securedsSocketCommunication.sendMessage(clientSocket,txtMsg);

					KVMessage kvmsg = processReply(securedsSocketCommunication.receiveMessage(clientSocket), StatusType.GET);

					long end = System.nanoTime();
					long err = System.nanoTime() - end;
					long time = end - start - err;

					double seconds = time/1000000000.0;

					statistics = statistics +" "+ String.valueOf(seconds);
					return kvmsg;

				} catch (IOException ioe) {
					logger.error("Unable to get value from KV server");
					throw new Exception("Unable to get value from KV server");
				}
			} else
				return this.redirected;
		} else {
			logger.error("Not connected to KV Server!");
			throw new Exception("Not connected to KV Server!");
		}
	}

	public void displayLatencyStatistics(String string) {
		System.out.println(string+" Latency >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
		System.out.println(statistics);
	}
}
