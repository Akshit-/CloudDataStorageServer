package server;
/**
 * Listener class for evaluating performance of KVServer.
 */
public interface PerformanceListener {

	/**
	 * Method to calculate throughput of KVServer.
	 * @param receivedBytes
	 * 		Number of bytes processed by KVServer
	 */
	public void calculateThroughput(int receivedBytes);
	
	/**
	 * Method to display throughput of KVServer.
	 * @return
	 * 		Returns throughput value of the KVServer.
	 */
	public String displayStatistics();
}
