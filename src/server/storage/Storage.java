package server.storage;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;


/**
 * Storage Module that uses ConcurrentHashMap as data structure.
 *  
 */
public class Storage {
	private static Storage s;
	private static Logger logger = Logger.getRootLogger();

	/*
	 * In Memory Storage with all operations being thread-safe and retrieval operations
	 * do not entail locking, resulting in faster access.
	 */
	ConcurrentHashMap<String, String> data;

	/*
	 * Map of Hash(key) and key of data stored.
	 */
	HashMap<String, String> keyHash;

	/**
	 * Initializes the storage
	 */
	public static Storage init() {
		if (s == null) {
			s = new Storage();
			logger.debug("Storage class created");
		}
		return s;
	}

	/**
	 * Constructor for Storage class.
	 */
	public Storage() {
		data = new ConcurrentHashMap<String, String>();
		keyHash = new HashMap<String, String>();
	}

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
	public synchronized String put(String key, String value) {
		logger.debug("Storage::put() + Storing kvpair: " + key + "," + value);
		//calculate hash of key and store it in keyHash map.
		String md5 = getMD5(key);
		keyHash.put(md5, key);
		return data.put(key, value);
	}

	/**
	 * Method for get operation on KVServer.
	 * 
	 * @param key
	 * 			key whose value is to retrieved.
	 * @return
	 * 			Returns value associated with the key.
	 */
	public synchronized String get(String key) {
		String result = data.get(key);
		logger.debug("Storage ("+""+" ::get() + Retrieved kvpair:" + key + "," + result);
		return result;
	}

	/**
	 * Method to delete key-value pair from KVServer.
	 * 
	 * @param key
	 *			key which is to be deleted.
	 * @return
	 * 		Returns value stored under the key. If no entry exists, returns null.
	 */
	public synchronized String delete(String key) {
		String result = data.remove(key);
		String md5 = getMD5(key);
		keyHash.remove(md5);
		logger.debug("Storage::delete() + Removed kvpair:" + key + "," + result);
		return result;
	}

	/**
	 * Retrieve a list of key value pairs present between the given range.
	 * 
	 * @param range
	 *            The range between which all values we need to return.
	 * @return HashMap of key-value pairs stored between this range. If no entry exists, null
	 */
	public synchronized HashMap<String, String> getDataBetweenRange(String range) {
		String startEnd[] = range.split(":");

		BigInteger start = new BigInteger(startEnd[0],16);
		BigInteger end = new BigInteger(startEnd[1],16);

		BigInteger maximum = new BigInteger("ffffffffffffffffffffffffffffffff",16);
		BigInteger minimum = new BigInteger("00000000000000000000000000000000",16);

		HashMap<String, String> dataToBeMoved = new HashMap<String, String>();

		logger.info("Storage::getDataBetweenRange()"
				+", new Server's start="+start
				+", new Server's end="+end
				+", Maximum ="+maximum
				+", Minimum ="+minimum);


		logger.info("Storage::getDataBetweenRange() + Data present currently on this server, size="+data.size());

		for(Iterator<Entry<String, String>>it=data.entrySet().iterator();it.hasNext();){
			Entry<String, String> entry = it.next();
			logger.info("Storage::getDataBetweenRange() + data: "+entry.getKey());
		}

		logger.info("Storage::getDataBetweenRange() + keyHash present currently on this server, size="+keyHash.size());

		for(Iterator<Entry<String, String>>it=keyHash.entrySet().iterator();it.hasNext();){
			Entry<String, String> entry = it.next();
			logger.info("Storage::getDataBetweenRange() + keyHash: "+entry.getValue());
		}	
		
		for(Iterator<Entry<String, String>>it=keyHash.entrySet().iterator();it.hasNext();){
			Entry<String, String> entry = it.next();
			BigInteger keyHash = new BigInteger(entry.getKey(),16);
			logger.info("Storage::getDataBetweenRange()+ Checking for keyHash="+keyHash+", value="+entry.getValue());

			if(start.compareTo(end)<0){
				logger.info("Storage::getDataBetweenRange()+ start<end case");
				if (keyHash.compareTo(start) > 0 && 
						keyHash.compareTo(end) <= 0){

					dataToBeMoved.put(entry.getValue(), data.get(entry.getValue()));
					logger.info("Storage::getDataBetweenRange(start<end) + dataToBeMoved="+entry.getValue()+", "+data.get(entry.getValue()));
				}
			}else{
				logger.info("Storage::getDataBetweenRange()+ start>end case");
				//startServer > endServer
				// keycheck1:: (startServer to MaximumRange) && keycheck2:: (0 to end)
				if((keyHash.compareTo(start) > 0 && keyHash.compareTo(maximum) <= 0 )
						|| (keyHash.compareTo(minimum) >= 0 && keyHash.compareTo(end) <= 0 )){

					dataToBeMoved.put(entry.getValue(), data.get(entry.getValue()));
					logger.info("Storage::getDataBetweenRange(start>end) + dataToBeMoved="+entry.getValue()+", "+data.get(entry.getValue()));
				}
			}
		}
		return dataToBeMoved;
	}

	/**
	 * Delete a map of data from Server's storage.
	 * 
	 * @param 	dataToBeDeleted
	 *        	The HashMap of key value pairs to be deleted from Storage.
	 * @return 	true
	 */
	public boolean deleteDataBetweenRange(HashMap<String, String> dataToBeDeleted) {
		logger.info("Storage:: deleteDataBetweenRange()");
		for(Iterator<Entry<String, String>>it=dataToBeDeleted.entrySet().iterator();it.hasNext();){
			Entry<String, String> entry = it.next();
			logger.info("Storage:: deleteDataBetweenRange() + deleting key,value="+entry.getKey()+","+entry.getValue());
			data.remove(entry.getKey());
			String md5 = getMD5(entry.getKey());
			keyHash.remove(md5);
		}
		return true;
	}

	/**
	 * Method to compute MD5
	 * 
	 * @param msg Value to be hashed
	 * @return hash value of msg
	 */
	private String getMD5(String msg){
		MessageDigest messageDigest = null;
		try {
			messageDigest = MessageDigest.getInstance("MD5");
		} catch(NoSuchAlgorithmException ex){
			logger.error("Storage::getMD5() + Error while computing MD5 for msg.");
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
}
