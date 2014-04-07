package common.security.cipher;

/**
 * 
 * Interface class to define functions of Cipher to be used for Encryption process.
 *
 */
public interface Cipher {
	/**
	 * Method to retrieve secret key for this Cipher.
	 * @return
	 */
	public String getSecretKey();
	
	/**
	 * Method used for encrypting a message.
	 * @param msg
	 * 		Message to be encrypted.
	 * @return
	 * 		Returns encrypted message.
	 */
	public String encrypt(String msg);
	
	/**
	 * Method used for decrypting a message.
	 * @param encryptedMsg
	 * 		Message to be decrypted.
	 * @return
	 * 		Returns decrypted message.
	 */
	public String decrypt(String encryptedMsg);

}
