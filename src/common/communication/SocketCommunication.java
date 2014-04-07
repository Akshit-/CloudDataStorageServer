package common.communication;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.Logger;

import common.messages.TextMessage;
import common.security.cipher.Cipher;
import common.security.cipher.aes.AES;

/**
 * This Class is used for Secure Socket communication between nodes.
 *
 */
public class SocketCommunication {

	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;

	private Cipher mCipher;
	private boolean encryption = false;

	public SocketCommunication() {

		this.encryption = true;

		if(encryption){
			mCipher = new AES();
		}

	}

	private static Logger logger = Logger.getRootLogger();

	/**
	 * Method sends a TextMessage using the socket.
	 *
	 * @param socket
	 *            the socket that is to be used to sent the message.
	 * @param msg
	 *            the message that is to be sent.
	 * @throws IOException
	 *             some I/O error regarding the output stream
	 */
	public void sendMessage(Socket socket, TextMessage msg) throws IOException {
		if(socket!=null){

			OutputStream output = socket.getOutputStream();
			byte[] msgBytes;

			if(encryption){
				String plaintext = msg.getMsg();
				String encryptedText = mCipher.encrypt(plaintext);

				TextMessage encryptedTextMsg = new TextMessage(encryptedText);

				msgBytes = encryptedTextMsg.getMsgBytes();

			}else{

				msgBytes = msg.getMsgBytes();

			}
			output.write(msgBytes);
			output.flush();
			logger.info("sendMessage() ="+msg.getMsg());
			logger.info("SEND \t<" + socket.getInetAddress().getHostAddress()
					+ ":" + socket.getPort() + ">: '" + msg.getMsg() + "'");
		}
	}

	/**
	 * Method for receiving a TextMessage using the socket.
	 * 
	 * @param socket 
	 * 				the socket that is to be used to receive message.
	 * @return TextMessage
	 * 				the message that is received.
	 * @throws IOException
	 * 				some I/O error regarding the output stream
	 */
	public TextMessage receiveMessage(Socket socket) throws IOException {


		InputStream input = socket.getInputStream();
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
		logger.info("receiveMessage() for:"+socket);
		byte read = (byte) input.read();
		boolean reading = true;

		while (read != 13 && reading) {/* carriage return */
			/* if buffer filled, copy to msg array */
			if (index == BUFFER_SIZE) {
				logger.info("receiveMessage-->index == BUFFER SIZE");
				if (msgBytes == null) {
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
							BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			}

			if ((read > 31 && read < 127)) {
				bufferBytes[index] = read;
				index++;
			}
			/* stop reading if DROP_SIZE is reached */
			if (msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				logger.error("receiveMessage-->DROP SIZE reached");
				reading = false;
			}

			/* read next char from stream */
			read = (byte) input.read();
		}

		if (msgBytes == null) {
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}

		msgBytes = tmp;

		TextMessage  msg = null;

		if(encryption){

			//decrypting text
			msg = new TextMessage(msgBytes);
			String encryptedText = msg.getMsg();
			logger.info("RECEIVE \t<"
					+ socket.getInetAddress().getHostAddress() + ":"
					+ socket.getPort() + ">: '" + msg.getMsg()+ "'"
					+ "=" + msgBytes + ",");


			String plainText = mCipher.decrypt(encryptedText);
			msg = new TextMessage(plainText);
			logger.info("(Decrypted)RECEIVE \t<"
					+ socket.getInetAddress().getHostAddress() + ":"
					+ socket.getPort() + ">: '" + msg.getMsg()+ "'"
					+ "=" + msgBytes + ",");
		}else{
			//decrypting text

			msg = new TextMessage(msgBytes);
			logger.info("RECEIVE \t<"
					+ socket.getInetAddress().getHostAddress() + ":"
					+ socket.getPort() + ">: '" + msg.getMsg()+ "'"
					+ "=" + msgBytes + ",");

		}

		return msg;

	}
}
