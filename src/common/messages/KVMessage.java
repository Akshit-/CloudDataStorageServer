package common.messages;

import java.util.List;

import metadata.MetaData;

/**
 * Interface specifying message protocol between KVServer and KVClient. 
 *
 */
public interface KVMessage {
	
	public enum StatusType {
		GET, 			/* Get - request */
		GET_ERROR, 		/* requested tuple (i.e. value) not found */
		GET_SUCCESS, 	/* requested tuple (i.e. value) found */
		PUT, 			/* Put - request */
		PUT_SUCCESS, 	/* Put - request successful, tuple inserted */
		PUT_UPDATE, 	/* Put - request successful, i.e. value updated */
		PUT_ERROR, 		/* Put - request not successful */
		DELETE_SUCCESS, /* Delete - request successful */
		DELETE_ERROR, 	/* Delete - request successful */
		SERVER_STOPPED,         /* Server is stopped, no requests are processed */
		SERVER_WRITE_LOCK,      /* Server locked for out, only get possible */
		SERVER_NOT_RESPONSIBLE,  /* Request not successful, server not responsible for key */
		REPLICA_PUT, 			/* Replica Put - request */
		REPLICA_PUT_SUCCESS, 	/* Replica Put - request successful, tuple inserted */
		REPLICA_PUT_UPDATE, 	/* Replica Put - request successful, i.e. value updated */
		REPLICA_PUT_ERROR, 		/* Replica Put - request not successful */
		REPLICA_DELETE_SUCCESS, /* Replica Delete - request successful */
		REPLICA_DELETE_ERROR, 	/* Replica Delete - request not successful */
		DELETE_TOPOLOGICAL,		/* Delete message from Successor Node to its 2nd successor*/
		UNKNOWN          /*Unknown command*/
	}

	/**
	 * Method to retrieve key associated with this KVMessage.
	 * @return 
	 * 		Returns key if present else null.
	 */
	public String getKey();
	
	/**
	 * Method to retrieve value associated with this KVMessage.
	 * @return 
	 * 		Returns value if present else null.
	 */
	public String getValue();
	
	/**
	 * Method to retrieve Status of this KVMessage.
	 * @return 
	 * 		Returns a status string that is used to identify request types, 
	 * 		response types and error types associated to the message.
	 */
	public StatusType getStatus();
	
	/**
	 * Method to retrieve list of every node MetaData.
	 * @return
	 * 		List of MetaData.
	 */
	public List<MetaData> getMetaData();
	
}


