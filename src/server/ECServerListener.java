package server;

import java.net.Socket;
import java.util.List;
import metadata.MetaData;

/**
 * This Class defines functions used to process messages received from ECServer.
 * 
 */
public interface ECServerListener {
	
	/**
     * This function Initialize the KVServer with 
     * the meta-data and block it for client requests.
     * 
     * @param metadata
     * 			list of all metadatas presently.
     */
	public void initKVServer(List<MetaData> metaDatas);
	
	/**
     * This starts the KVServer, all client requests and all ECS requests are processed.
     * 
     */
	public void startKVServer();
	
	
	/** 
     * This Stops the KVServer, all client requests are rejected and only ECS requests are processed.
     * 
     */	
	public void stopKVServer();
		
	 /**
     * This function will Lock the KVServer for write operations.
     * 
     */
	public void lockWrite();
	
	
	/**
     * This function will Lock the KVServer for write operations.
     * 
     */
	public void unlockWrite();
	
	/**
     * This function will move a subset (range) of the KVServer’s data to new Server
	 * @param server KVServer where data is to be moved.
	 * @param replication 
	 * @return true if successfully moved else false.
     * 
     */
	public boolean moveData(String range, String server, boolean replication);
	
	/**
	 * This function will Update the meta-data repository of this server.
	 * @param metadatas
	 * 			updated list of metadatas.
	 */
	public void update(List<MetaData> metadatas);

	/**
	 * Get KVServer current status for Client connection.
	 * @return
	 * 		true if KVServer is active for clients else false
	 */
	public boolean isActiveForClients();
	
	/**
	 * Checks whether KVServer has been locked by ECServer or not.
	 * @return
	 * 		true if locked by ECServer else false
	 */
	public boolean isLockWrite();
	
	/**
	 * This function will return KVServer's Meta Data.
	 * @return metaData corresponding to KVServer.
	 */
	public MetaData getNodeMetaData();
	
	/**
	 * This function return list of all Meta Data's
	 * @return list of Meta Data's
	 */
	public List<MetaData> getServiceMetaData();
	
	/**
	 * This function will delete data present in the range of this MetaData
	 * @return true if delete operation was successful else false
	 */
	public boolean deleteDataBetween(MetaData mdata);
	
	/**
	 * Method to retrieve metaData of 1st Replica of this KVServer.
	 * @return 
	 * 		metaData of 1st Replica Server.
	 */
	public MetaData getMyReplica1MetaData();
	
	/**
	 * Method to retrieve metaData of 2nd Replica of this KVServer.
	 * @return
	 * 		metaData of 2nd Replica Server.
	 */
	public MetaData getMyReplica2MetaData();
	
	/**
	 * Method to retrieve socket of 1st Replica of this KVServer.
	 * @return
	 * 		socket of 1st Replica Server.
	 */
	public Socket getMyReplica1Socket();
	
	/**
	 * Method to retrieve socket of 2nd Replica of this KVServer.
	 * @return
	 * 		socket of 2nd Replica Server.
	 */
	public Socket getMyReplica2Socket();
	
	/**
	 * Method to set socket of 1st Replica of this KVServer.
	 * 
	 */
	public void setMyReplica1Socket(Socket s);
	
	/**
	 * Method to set socket of 2nd Replica of this KVServer.
	 * 
	 */
	public void setMyReplica2Socket(Socket s);
	
	/**
	 * Method to retrieve metaData of 1st node whose Data is replicated in this KVServer.
	 * @return
	 * 		metaData of Coordinator Node.
	 */
	public MetaData getReplica1MetaData();
	
	/**
	 * Method to retrieve metaData of 2nd node whose Data is replicated in this KVServer.
	 * @return
	 * 		metaData of Coordinator Node.
	 */
	public MetaData getReplica2MetaData();
	
	/**
	 * Method to replicate Data to another Server.
	 * @param socket
	 * 			Server's socket where to move the data.
	 * @param range
	 * 			range in the ring.
	 */
	void replicateDataToServer(Socket socket, String range);


}
