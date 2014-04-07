package common.messages;

import java.util.List;

import metadata.MetaData;

/**
 * Interface specifying message protocol between KVServer and ECServer.
 *
 */
public interface KVAdminMessage {
	
	public enum Commands {
		INIT, 				/* Initialize KVServer */
		INIT_SUCCESS,		/* Initialize KVServer was success*/
		INIT_FAIL,			/* Initialize KVServer was failure*/
		START, 				/* Starts KVServer */
		START_SUCCESS, 		/* Starts KVServer was success*/
		START_FAIL, 		/* Starts KVServer was failure*/
		STOP, 				/* Stop KVServer */
		STOP_SUCCESS, 		/* Stop KVServer was success*/
		STOP_FAIL, 			/* Stop KVServer was failure*/
		SHUTDOWN, 			/* Exits the KVServer application */
		SHUTDOWN_SUCCESS, 	/* Exits the KVServer application was success*/
		SHUTDOWN_FAIL, 		/* Exits the KVServer application was failure*/
		LOCK_WRITE, 		/* Lock the KVServer for write operations */
		LOCK_WRITE_SUCCESS, /* Lock the KVServer for write operations was success*/
		LOCK_WRITE_FAIL, 	/* Lock the KVServer for write operations was failure*/
		UNLOCK_WRITE, 	    /* UnLock the KVServer for write operations */
		UNLOCK_WRITE_SUCCESS, 	/* UnLock the KVServer for write operations was success*/
		UNLOCK_WRITE_FAIL, 	/* UnLock the KVServer for write operations was failure*/
		MOVE_DATA, 		    /* Transfer a range of the KVServer's data to another */
		MOVE_DATA_REPLICATE, /* Replicated Transfer of range of the KVServer's data to another */
		MOVE_DATA_SUCCESS, 	/* Transfer a range of the KVServer’s data to another was success*/
		MOVE_DATA_FAIL, 	/* Transfer a range of the KVServer’s data to another was failure*/
		UPDATE, 			/* Update the meta-data repository of this server */
		UPDATE_SUCCESS, 	/* Update the meta-data repository of this server was success*/
		UPDATE_FAIL, 		/* Update the meta-data repository of this server was failure*/
		REPLICATE,			/* Command to replicate a given range of Data to a given Server*/
		REPLICATE_SUCCESS,	/* Success of REPLICATE*/
		PING,               /* Used for fault detection*/
		ECHO,               /* PING SUCCESS ECHO for fault detection*/
		UNKNOWN				/*Unknown Command*/
	}
	
	/** 
	 * Method to retrieve list of all MetaDatas.
	 * @return 
	 * 		List of MetaDatas.
	 */
	public List<MetaData> getMetaDatas();
	
	/**
	 * Method to get Command associated with the KVAdminMessage.
	 * @return 
	 * 		ECServer command
	 */
	public Commands getCommand();
	
	/**
	 * Method to get Range of Data to be moved.
	 * @return Range of Hash for MoveData process
	 */
	public String getRange();
	
	/**
	 * Method to get Address of KVServer where data is to be moved.
	 * @return 
	 * 		Destination KVServer Address.
	 */
	public String getDestinationAddress();
	
	
}
