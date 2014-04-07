package ecs;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

import org.apache.log4j.Logger;

import common.communication.SocketCommunication;
import common.messages.JSONSerializer;
import common.messages.KVAdminMessage;
import common.messages.TextMessage;
import common.messages.KVAdminMessage.Commands;

/**
 * FaultDetector class for detecting failing nodes through probing mechanism.
 *
 */
public class FaultDetecter extends Thread {

	private boolean stop;
	private ECServer mECServer;
	private List<Socket> mEcsClientSockets;
	private SocketCommunication socketCommunication;

	//Fault timeout
	private static final long FAULT_DETECTION_TIMEOUT = 10000;

	//logger
	private static Logger logger = Logger.getRootLogger();

	public FaultDetecter(ECServer ecServer) {
		mECServer = ecServer;
		socketCommunication = new SocketCommunication();
		stop = false;
	}

	public boolean isStop() {
		return stop;
	}

	public void setStop(boolean stop) {
		this.stop = stop;
	}

	public void run(){

		while(!stop) {

			try {
				mEcsClientSockets = mECServer.getECSServerSockets();
				for(Socket socket: mEcsClientSockets){
					try {
						TextMessage txtMsg = JSONSerializer.marshalKVAdminMsg(null, Commands.PING, "","");
						socketCommunication.sendMessage(socket, txtMsg);

						TextMessage responseTxtMsg = socketCommunication.receiveMessage(socket);
						logger.debug("FaultDetecter-->response from KVServer:"+responseTxtMsg.getMsg());

						KVAdminMessage responseKVAdminMsg = JSONSerializer.unmarshalKVAdminMsgForCommand(responseTxtMsg);
						logger.debug("FaultDetecter-->UnMarshaled response from KVServer:"
								+socket.getInetAddress().getHostAddress()
								+":"+socket.getPort()
								+" is "+responseKVAdminMsg.getCommand().toString());

					} catch (IOException e) {
						logger.error("FaultDetecter-->IOException while sending start command to KVServer:"
								+socket.getInetAddress().getHostAddress()
								+":"+socket.getPort());

						//informing ECServer about failing node
						if(!stop) mECServer.handleFaultyServer(socket);		
						break;
					} 
				}
				//Run fault detection every Timeout
				Thread.sleep(FAULT_DETECTION_TIMEOUT);
			} catch (InterruptedException e1) {
				logger.error("FaultDetecter-->InterruptedException in thread");
			}
		}
	}

}
