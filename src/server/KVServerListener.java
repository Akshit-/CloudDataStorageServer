package server;

/**
 * This Class defines functions used for handling Client's request.
 *
 */
public interface KVServerListener {

	/**
	 * Method for put operation on KVServer.
	 * 
	 * @param key 
	 * 			key to be inserted.
	 * @param value
	 * 			value to be inserted.
	 * @return 
	 * 			Returns previous value stored else return null.
	 */
	public String put(String key, String value);
	
	/**
	 * Method for get operation on KVServer.
	 * 
	 * @param key
	 * 			key whose value is to retrieved.
	 * @return
	 * 			Returns value associated with the key.
	 */
	public String get(String key);
	
	/**
	 * Method to delete key-value pair from KVServer.
	 * 
	 * @param key
	 *			key which is to be deleted.
	 * @return
	 * 		Returns value stored under the key. If no entry exists, returns null.
	 */
	public String delete(String key);

}
