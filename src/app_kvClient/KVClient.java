package app_kvClient;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.io.IOException;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.TextMessage;
import client.ClientSocketListener;
import client.KVStore;


public class KVClient implements ClientSocketListener{
	private static Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "KVClient> ";
	private BufferedReader stdin;
	private KVStore mKVStore = null;
	private boolean stop = false;

	private String serverAddress;
	private int serverPort;

	public void run() {
		while(!stop) {
			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(PROMPT);

			try {
				String cmdLine = stdin.readLine();
				this.handleCommand(cmdLine);
			} catch (IOException e) {
				stop = true;
				printError("CLI does not respond - Application terminated ");
			}
		}
	}

	private void handleCommand(String cmdLine) {
		if(cmdLine != null) {
			String[] tokens = cmdLine.split("\\s+");

			if(tokens[0].equals("quit")) {	
				stop = true;
				disconnect();
				System.out.println(PROMPT + "Application exit!");

			} else if (tokens[0].equals("connect")){
				if(tokens.length == 3) {
					try{
						serverAddress = tokens[1];
						serverPort = Integer.parseInt(tokens[2]);
						connect(serverAddress, serverPort);
					} catch(NumberFormatException nfe) {
						printError("No valid address. Port must be a number!");
						logger.info("Unable to parse argument <port>", nfe);
					} catch (UnknownHostException e) {
						printError("Unknown Host!");
						logger.info("Unknown Host!", e);
					} catch (IOException e) {
						printError("Could not establish connection!");
						logger.warn("Could not establish connection!", e);
					}
				} else {
					printError("Invalid number of parameters!");
				}
			} else if(tokens[0].equals("put")){  

				if(mKVStore==null){
					printError("Not connected with KVServer!");
					return;
				}

				if(tokens.length==3 && !tokens[1].isEmpty() && tokens[1].length()<=20){

					if(!tokens[2].equalsIgnoreCase("null")) {
						try {

							KVMessage kvMsg=mKVStore.put(tokens[1], tokens[2]);

							if(kvMsg.getStatus()==StatusType.PUT_SUCCESS){
								handleNewMessage("PUT_SUCCESS : Added Key-value pair added on KVServer");
							} else if (kvMsg.getStatus()==StatusType.PUT_UPDATE){
								handleNewMessage("PUT_UPDATE : Updated key-value pair on KVServer");
							} else if(kvMsg.getStatus()==StatusType.PUT_ERROR){
								handleNewMessage("PUT_ERROR : Unable to add Key-value pair on KVServer");
							}else{
								printError("Unable to add Key-value pair on KVServer with status="+kvMsg.getStatus().toString());
							}
						} catch (Exception e) {
							printError("Unable to add Key-value pair on KVServer");
						}
					} else {
						try {
							KVMessage kvMsg=mKVStore.put(tokens[1], "");
							if(kvMsg.getStatus()==StatusType.DELETE_SUCCESS){
								handleNewMessage("DELETE_SUCCESS : Key deleted from server.");
							} else if (kvMsg.getStatus()==StatusType.DELETE_ERROR){
								handleNewMessage("DELETE_ERROR : Key not found on server.");
							}else{
								printError("Unable to delete Key from KVServer with status="+kvMsg.getStatus().toString());
							}
						} catch (Exception e) {
							printError("Unable to delete key-value pair on KVServer");
						}
					} 

				} else if (tokens.length==3 && !tokens[1].isEmpty() && tokens[1].length()>20){
					printError("The key size must not exceed 20 characters.");
				} else {
					printError("Invalid number of parameters!");
				}
			} else if(tokens[0].equals("get")){

				if(mKVStore==null){
					printError("Not connected with KVServer!");
					return;
				}

				if(tokens.length==2 && !tokens[1].isEmpty() && tokens[1].length()<=20){
					try {
						KVMessage kvMsg=mKVStore.get(tokens[1]);
						if(kvMsg.getStatus()==StatusType.GET_SUCCESS){
							handleNewMessage("GET_SUCCESS : value = "+kvMsg.getValue());
						} else if(kvMsg.getStatus()==StatusType.GET_ERROR) {
							handleNewMessage("GET_ERROR : Key not found on KV server.");
						}else{
							printError("Unable to get Key-value pair from KVServer with status="+kvMsg.getStatus().toString());
						}
					} catch (Exception e) {
						printError("Unable to get value for the given key from KVServer");
					}
				} else if (tokens.length==2 && !tokens[1].isEmpty() && tokens[1].length() > 20){
					printError("The key size must not exceed 20 characters.");
				} else {
					printError("Invalid number of parameters!");
				}
			} else if(tokens[0].equals("disconnect")) {
				if(mKVStore==null){
					printError("Not connected with KVServer!");
					return;
				}
				disconnect();

			} else if(tokens[0].equals("logLevel")) {
				if(tokens.length == 2) {
					String level = setLevel(tokens[1]);
					if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
						printError("No valid log level!");
						printPossibleLogLevels();
					} else {
						System.out.println(PROMPT + 
								"Log level changed to level " + level);
					}
				} else {
					printError("Invalid number of parameters!");
				}

			} else if(tokens[0].equals("help")) {
				printHelp();
			} else {
				printError("Unknown command");
				printHelp();
			}
		}
	}

	private void connect(String address, int port) 
			throws UnknownHostException, IOException {
		mKVStore = new KVStore(address, port);
		mKVStore.addListener(this);
		try {
			mKVStore.connect();
			handleNewMessage("Connection to MSRG KV server established: "
					+ address + " / " + port);
		} catch (Exception e) {
			printError("Unable to connect to KVServer: /"+address+":"+port);
		}
	}

	private void disconnect() {
		if(mKVStore != null) {
			mKVStore.disconnect();
			handleNewMessage("Disconnect from KVServer!");
			mKVStore = null;
		}
	}

	private void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append(PROMPT).append("ECHO CLIENT HELP (Usage):\n");
		sb.append(PROMPT);
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append(PROMPT).append("connect <host> <port>");
		sb.append("\t establishes a connection to a server\n");
		sb.append(PROMPT).append("put <key> <value>");
		sb.append("\t\t Puts key value pair on server. \n");
		sb.append("\t\t Deletes key value pair if value equals null. \n");

		sb.append(PROMPT).append("get <key>");
		sb.append("\t\t Gets value corresponding to the key from server. \n");

		sb.append(PROMPT).append("disconnect");
		sb.append("\t\t\t disconnects from the server \n");

		sb.append(PROMPT).append("logLevel");
		sb.append("\t\t\t changes the logLevel \n");
		sb.append(PROMPT).append("\t\t\t\t ");
		sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

		sb.append(PROMPT).append("quit ");
		sb.append("\t\t\t exits the program");
		System.out.println(sb.toString());
	}

	private void printPossibleLogLevels() {
		System.out.println(PROMPT 
				+ "Possible log levels are:");
		System.out.println(PROMPT 
				+ "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
	}

	private String setLevel(String levelString) {

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

	@Override
	public void handleNewMessage(TextMessage msg) {
		if(!stop) {
			System.out.print(PROMPT);
			System.out.println(msg.getMsg());
		}
	}

	public void handleNewMessage(String msg) {
		if(!stop) {
			System.out.print(PROMPT);
			System.out.println(msg);
		}
	}

	@Override
	public void handleStatus(SocketStatus status) {
		if(status == SocketStatus.CONNECTED) {

		} else if (status == SocketStatus.DISCONNECTED) {
			System.out.print(PROMPT);
			System.out.println("Connection terminated: " 
					+ serverAddress + " / " + serverPort);

		} else if (status == SocketStatus.CONNECTION_LOST) {
			System.out.print(PROMPT);
			System.out.println("Connection lost: " 
					+ serverAddress + " / " + serverPort);
		}

	}

	private void printError(String error){
		System.out.println(PROMPT + "Error! " +  error);
	}

	/**
	 * Main entry point for the echo server application. 
	 * @param args contains the port number at args[0].
	 */
	public static void main(String[] args) {
		try {
			new LogSetup("logs/client/client.log", Level.ALL);
			KVClient app = new KVClient();
			app.run();
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			System.exit(1);
		}
	}

}
