package ecs;

/**
 * Meta class associated with each server node used by ECServer.
 *
 */
public class ServerNodeData {
	private String nodeName;
	private String ipAddress;
	private String port;
	
	//for failure detection
	private boolean alive;
	
	public ServerNodeData(String nodeName, String ipAddress, String port, boolean alive){
		this.nodeName=nodeName;
		this.ipAddress=ipAddress;
		this.port=port;
		this.alive=alive;
	}
	
	public boolean isAlive() {
		return alive;
	}

	public void setAlive(boolean alive) {
		this.alive = alive;
	}

	public String getNodeName() {
		return nodeName;
	}
	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}
	public String getIPAddress() {
		return ipAddress;
	}
	public void setIPAddress(String ipAddress) {
		this.ipAddress = ipAddress;
	}
	public String getPort() {
		return port;
	}
	public void setPort(String port) {
		this.port = port;
	}

}
