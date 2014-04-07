package app_kvEcs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import ecs.ECServer;

public class ECSClient {

	private int mNodeCount=0;
	private boolean mStorageServiceRunning = false;
	private boolean mStorageServiceInitiated = false;

	private static Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "ECSClient> ";
	private BufferedReader stdin;
	private boolean stop=false;
	private ECServer mECSServer;
	public ECSClient(String string) {

		mECSServer = new ECServer(string);

	}

	public static void main(String[] args) {
		try {
			new LogSetup("logs/ecs/ecsclient.log", Level.ALL);
			if(args!=null&&args.length==1){
				ECSClient app = new ECSClient(args[0]);
				app.readCommandFromConsole();
			} else {
				System.out.println("Error! Invalid number of parameters. Parameter count should be 1.");
				System.exit(1);
			}

		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}

	}

	public void readCommandFromConsole() {
		while(!stop) {
			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(PROMPT);

			try {
				String cmdLine = stdin.readLine();
				handleCommand(cmdLine);
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
				if(mStorageServiceInitiated) {
					mECSServer.shutDown();
					mStorageServiceInitiated = false;
					mStorageServiceRunning = false;
					mECSServer = null;

				}

				stop = true;
				System.out.println(PROMPT + "Application exit!");

			} else if (tokens[0].equals("initService")){
				if(tokens.length == 2) {
					try{

						mNodeCount = Integer.parseInt(tokens[1]);
						if(!mStorageServiceInitiated && mECSServer.getMaxAvailableNodeCount() >= mNodeCount) {
							boolean result = mECSServer.initService(mNodeCount);
							if(result) {
								printNewMessage("Storage service is initiated.");
								mStorageServiceInitiated=true;
							} else {
								printError("Unable to initialize storage service due to internal error.");
							}

						} else if(mStorageServiceInitiated){
							printError("Illegal Operation! Service Already initialized.");
							printError("First shutdown storage service by \"shutDown\" command");
						} else {
							printError("Unable to initialize storage service since given number of nodes : "+mNodeCount+" is more than total available nodes : "+ mECSServer.getMaxAvailableNodeCount());
						}
					} catch(NumberFormatException nfe) {
						printError("Invalid argument! Number of nodes should be between 1 to 8.");
						logger.info("Unable to parse argument <port>", nfe);
					} 
				} else {
					printError("Invalid number of parameters!");
				}

			} else  if (tokens[0].equals("start")) {
				if(tokens.length == 1) {
					if(mStorageServiceInitiated) { 
						boolean result = mECSServer.start();
						if(result) {
							printNewMessage("Storage service is started. Clients can access KVServers now.");
							mStorageServiceRunning=true;
						} else {
							printError("Unable to start storage service due to internal error.");
						}

					} else {
						printError("Illegal Operation! First initialize storage service by \"initService\" command");
					}


				} else {
					printError("Invalid number of parameters!");
				}

			} else  if (tokens[0].equals("stop")) {
				if(tokens.length == 1) {
					if(mStorageServiceRunning) {
						boolean result = mECSServer.stop();
						if(result) {
							printNewMessage("Storage service is stopped!");
							mStorageServiceRunning = false;
						} else {
							printError("Unable to stop storage service due to internal error.");
						}

					} else {
						printError("Illegal Operation! First start storage service by \"start\" command");
					}

				} else {
					printError("Invalid number of parameters!");
				}

			} else  if (tokens[0].equals("shutdown")) {
				if(tokens.length == 1) {
					if(mStorageServiceInitiated) {
						boolean result = mECSServer.shutDown();
						if(result) {
							printNewMessage("Storage service is shutdown!");
							mStorageServiceRunning = false;
							mStorageServiceInitiated = false;
						} else {
							printError("Unable to shutdown storage service due to internal error.");
						}

					} else {
						printError("Illegal Operation! No storage servers are running. First initialize the storage service by \"initService\" command.");	
					}

				} else {
					printError("Invalid number of parameters!");
				}

			} else  if (tokens[0].equals("addNode")) {
				if(tokens.length == 1) {
					if(mStorageServiceRunning) {
						if(mECSServer.getActivatedNodeCount() < mECSServer.getMaxAvailableNodeCount()) {
							boolean result = mECSServer.addNode();
							if(result) {
								printNewMessage("Added node successfully!");
							} else {
								printError("Unable to add new node due to internal error.");
							}
						} else {
							printError("Cannot add more nodes; Maximum node count  reached.");
						}

					} else {
						printError("Illegal operation!  No storage servers are running. "
								+ "First start the storage service by \"start\" command.");
					}

				} else {
					printError("Invalid number of parameters!");
				}

			} else  if (tokens[0].equals("removeNode")) {
				if(tokens.length == 1) { 
					if(mStorageServiceRunning) {
						int numServer = mECSServer.getActivatedNodeCount();
						if(numServer > 0) {
							boolean result = mECSServer.removeNode();
							if(result) {
								if(mECSServer.getActivatedNodeCount()==0) {
									printNewMessage("Removed the last node successfully, no nodes are running now.");
									printNewMessage("Initialize service again by initService <nodeCount> before running any other commands.");
									mStorageServiceRunning =false;
									mStorageServiceInitiated = false;
								} else {
									printNewMessage("Removed node successfully!");
								}
							} else {
								printError("Unable to remove a node due to internal error.");
							}
						} else {
							if(numServer==3){
								printError("Cannot remove more nodes now;Number of node running="+numServer);
							}else{
								printError("Cannot remove node; No node is running.");
							}
						}
					} else {
						printError("Illegal operation!  No storage servers are running."
								+ " First start the storage service by \"start\" command.");
					}
				} else {
					printError("Invalid number of parameters!");
				}

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

	private void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append("ECS CLIENT HELP (Available Commands):\n");
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append("\ninitService <number of Nodes>\n");
		sb.append("Initializes KV Service with given number of nodes.\n");
		sb.append("\nstart\n");
		sb.append("Start KV Service for client nodes.\n");
		sb.append("\nstop\n");
		sb.append("Stop KV Service for client nodes. \n");
		sb.append("\naddNode\n");
		sb.append("Adds one KV Server node to the service and starts it.\n");
		sb.append("\nremoveNode\n");
		sb.append("Removes one KV Server node from the service and ends the process running it.\n");
		sb.append("\nshutdown\n");
		sb.append("Shutdowns every node and ends all the process running KV Servers.\n");
		sb.append("\nlogLevel <level>\n");
		sb.append("Changes the logLevel \n");
		sb.append("Available levels are : ");
		sb.append("<ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF> \n");
		sb.append("\nquit\n");
		sb.append("Exits the program\n");
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



	public void printNewMessage(String msg) {
		if(!stop) {
			System.out.print(PROMPT);
			System.out.println(msg);
		}
	}



	private void printError(String error){
		System.out.println(PROMPT + "Error! " +  error);
	}


}
