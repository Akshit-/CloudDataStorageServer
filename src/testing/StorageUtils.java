package testing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;

public class StorageUtils {

	/**
	 * 
	 * @param file
	 * @return content
	 * @throws Exception
	 */
	public static String getContent(String file) throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(file));
		try {
			StringBuilder sb = new StringBuilder();
			String line = br.readLine();

			while (line != null) {
				sb.append(line);
				sb.append('\n');
				line = br.readLine();
			}
			return sb.toString();
		} finally {
			br.close();
		}
	}

	public static HashMap<String, String> storeDataSet(final File folder)
			throws Exception {

		HashMap<String, String> data = new HashMap<String, String>();
		for (final File fileEntry : folder.listFiles()) {
			if (fileEntry.isDirectory()) {
				storeDataSet(fileEntry);
			} else {
				data.put(fileEntry.getName(), getContent(folder.getAbsolutePath()+"/"+fileEntry.getName()));
			}
		}
		return data;
	}
}
