package testing;

import junit.framework.TestCase;
import org.junit.Test;
import common.security.cipher.Cipher;
import common.security.cipher.aes.AES;

public class CipherTest extends TestCase {

	@Test
	public void testCipher() {
		Cipher cipher = new AES();		
		String txt = "Message to be encrypted";
		
		String encryptedTxt = cipher.encrypt(txt);
	
		String decryptedTxt = cipher.decrypt(encryptedTxt);
		System.out.println("Original text: "+txt);
		System.out.println("Encrypted text: "+encryptedTxt);
		System.out.println("Decrypted text: "+decryptedTxt);
		
		assertTrue(txt.equals(decryptedTxt));

		
	}
	

}