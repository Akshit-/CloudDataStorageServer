package common.security.cipher.aes;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;


/**
 * AES encryption class for secure communication of messages. 
 *
 */
public class AES implements common.security.cipher.Cipher {

	private Cipher mAESCipher;
	private static SecretKey mKey;
	private String mKeyString;

	private static Logger logger = Logger.getRootLogger();

	public AES() {

		if(mKey==null){

			File cmdLineConfig = new File("secret_key");
			BufferedReader reader = null;
			try {
				reader = new BufferedReader(new FileReader(cmdLineConfig));
			} catch (FileNotFoundException e1) {
				logger.error("Secret_Key file not fount at : "+cmdLineConfig.getPath());
				System.out.println("Secret_Key  file not fount at : "+cmdLineConfig.getPath());
			}
			
			String line = null;
			try {
				while ((line = reader.readLine()) != null) {
					setSecretKey(line);;
				}
			} catch (IOException e) {
				logger.error("IOException while reading the secret key file.");
				System.out.println("IOException while reading the secret key file.");
			}

		}

		try {
			mAESCipher = Cipher.getInstance("AES");
		} catch (NoSuchAlgorithmException e) {
			logger.error("AES::AES() + NoSuchAlgorithmException while creating Cipher");
		} catch (NoSuchPaddingException e) {
			logger.error("AES::AES() + NoSuchPaddingException while creating Cipher");
		}

	}
	private void setSecretKey(String key) {
		mKeyString = key;
		byte[] keyByte;
		try {
			keyByte = key.getBytes("UTF-8");
			MessageDigest md5 = null;
			try {
				md5 = MessageDigest.getInstance("MD5");
				keyByte = md5.digest(keyByte);
				keyByte = Arrays.copyOf(keyByte, 16); // use only first 128 bit
				mKey = new SecretKeySpec(keyByte, "AES");
			} catch (NoSuchAlgorithmException e) {
				logger.error("AES::setSecretKey() + NoSuchAlgorithmException while setting key for Cipher");
			}
		} catch (UnsupportedEncodingException e1) {
			logger.error("AES::setSecretKey() + UnsupportedEncodingException while setting key for Cipher");
		}
	}

	@Override
	public String getSecretKey() {
		return mKeyString;
	}

	@Override
	public String encrypt(String msg) {
		try {
			mAESCipher.init(Cipher.ENCRYPT_MODE, mKey);
			byte[] encrypted = mAESCipher.doFinal(msg.getBytes());
			return Base64.encodeBase64String(encrypted);
		} catch (InvalidKeyException e) {
			logger.error("AES::encrypt() + InvalidKeyException while encryption");
		} catch (IllegalBlockSizeException e) {
			logger.error("AES::encrypt() + IllegalBlockSizeException while encryption");
		} catch (BadPaddingException e) {
			logger.error("AES::encrypt() + BadPaddingException while encryption");
		} 
		return null;
	}

	@Override
	public String decrypt(String encryptedMsg) {
		try {
			mAESCipher.init(Cipher.DECRYPT_MODE, mKey);
			byte[] decrypted = mAESCipher.doFinal(Base64.decodeBase64(encryptedMsg));
			return new String(decrypted);
		} catch (InvalidKeyException e) {
			logger.error("AES::decrypt() + InvalidKeyException while decryption");
		} catch (IllegalBlockSizeException e) {
			logger.error("AES::decrypt() + IllegalBlockSizeException while decryption");
		} catch (BadPaddingException e) {
			logger.error("AES::decrypt() + BadPaddingException while decryption");
		}
		return null;
	}
}
