package metadata;
/**
 * Meta Data class for server node used for communication between Client and Server. 
 *
 */
public class MetaData {

	String ip;
	String port;

	String rangeStart;
	String rangeEnd;

	public MetaData(String ip, String port, String rangeStart, String rangeEnd) {
		this.ip = ip;
		this.port = port;
		this.rangeStart = rangeStart;
		this.rangeEnd = rangeEnd;

	}

	public MetaData() {
	}

	public String getIP() {
		return ip;
	}

	public void setIP(String ip) {
		this.ip = ip;
	}

	public String getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = port;
	}

	public String getRangeStart() {
		return rangeStart;
	}

	public void setRangeStart(String rangeStart) {
		this.rangeStart = rangeStart;
	}

	public String getRangeEnd() {
		return rangeEnd;
	}

	public void setRangeEnd(String rangeEnd) {
		this.rangeEnd = rangeEnd;
	}

	public boolean equals(MetaData metadata){
		if(this.ip.equals(metadata.ip)
				&& this.port.equals(metadata.port)
				&& this.rangeStart.equals(metadata.rangeStart)
				&& this.rangeEnd.equals(metadata.rangeEnd)){
			return true;
		}
		return false;
	}

}
