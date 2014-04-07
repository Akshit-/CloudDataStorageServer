package server;

import java.util.List;

import metadata.MetaData;
/**
 * Server class thread-safe lazy-initialization, 
 * represents all information related with KVServer.
 *
 */
public class Server {

	private static class Loader {
		static Server INSTANCE = new Server();
	}
	/**
	 * Making it singleton
	 */
	private Server() {
	}

	public static Server getInstance() {
		return Loader.INSTANCE;
	}
	
	/**
	 * Status for responding to client calls.
	 */
	private boolean isActiveForClients;

	/**
	 * Status for blocking write calls
	 */
	private boolean isLockWrite;

	/**
	 * Complete set of meta data
	 */
	private List<MetaData> serviceMetaData;

	/**
	 * Coordinator node meta data
	 */
	private MetaData nodeMetaData;
	
	/**
	 * Replica 1 meta data
	 */
	private MetaData replica1MetaData;
	
	/**
	 * Replica 2 meta data
	 */
	private MetaData replica2MetaData;
	
	/**
	 * This server's Replica 1 node meta data
	 */
	private MetaData myReplica1MetaData;
	
	/**
	 * This server's Replica 1 node meta data
	 */
	private MetaData myReplica2MetaData;
	

	public boolean isLockWrite() {
		return isLockWrite;
	}

	public void setLockWrite(boolean isLockWrite) {
		this.isLockWrite = isLockWrite;
	}
	
	public boolean isActiveForClients() {
		return isActiveForClients;
	}

	public void setIsActiveForClients(boolean isLockWrite) {
		this.isActiveForClients = isLockWrite;
	}
	
	
	public List<MetaData> getServiceMetaData() {
		return serviceMetaData;
	}

	public void setServiceMetaData(List<MetaData> serviceMetaData) {
		this.serviceMetaData = serviceMetaData;
	}

	public MetaData getNodeMetaData() {
		return nodeMetaData;
	}

	public MetaData getReplica1MetaData() {
		return replica1MetaData;
	}
	
	public MetaData getReplica2MetaData() {
		return replica2MetaData;
	}
	
	public MetaData getMyReplica1MetaData() {
		return myReplica1MetaData;
	}
	
	public MetaData getMyReplica2MetaData() {
		return myReplica2MetaData;
	}
	
	public void setNodeMetaData(MetaData nodeMetaData) {
		this.nodeMetaData = nodeMetaData;
	}

	public void setReplica1MetaData(MetaData replica1MetaData) {
		this.replica1MetaData = replica1MetaData;
	}
	
	public void setReplica2MetaData(MetaData replica2MetaData) {
		this.replica2MetaData = replica2MetaData;
	}
	
	public void setMyReplica1MetaData(MetaData myReplica1MetaData) {
		this.myReplica1MetaData = myReplica1MetaData;
	}
	
	public void setMyReplica2MetaData(MetaData myReplica2MetaData) {
		this.myReplica2MetaData = myReplica2MetaData;
	}
}
