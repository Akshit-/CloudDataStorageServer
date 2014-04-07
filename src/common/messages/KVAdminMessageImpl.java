package common.messages;

import java.util.List;
import metadata.MetaData;

public class KVAdminMessageImpl implements KVAdminMessage {
	
	
	private List<MetaData> metaDatas;
	private Commands command;
	private String range;
	private String destinationAddress;
	
	public KVAdminMessageImpl(List<MetaData> metaDatas, Commands commands, String range, String destinationAddress) {
		this.metaDatas = metaDatas;
		this.command = commands;
		this.range = range;
		this.destinationAddress = destinationAddress;
	}
	
	public KVAdminMessageImpl(){
	
	}

	public Commands getCommand() {
		return command;
	}

	public void setCommand(Commands commands) {
		this.command = commands;
	}

	public String getRange() {
		return range;
	}

	public void setRange(String range) {
		this.range = range;
	}

	public String getDestinationAddress() {
		return destinationAddress;
	}

	public void setDestinationAddress(String ip_port) {
		this.destinationAddress = ip_port;
	}

	
	public List<MetaData> getMetaDatas() {
		return metaDatas;
	}
	
	
	public void setMetaDatas(List<MetaData> metaDatas) {
		this.metaDatas = metaDatas;
		
	}	
	
	
	
	public static Commands getCommandType(int command){
	
		switch(command){
			
			case 0: return Commands.INIT;			
			case 1: return Commands.INIT_SUCCESS;
			case 2: return Commands.INIT_FAIL;
			case 3: return Commands.START;
			case 4: return Commands.START_SUCCESS;
			case 5: return Commands.START_FAIL;
			case 6: return Commands.STOP;
			case 7: return Commands.STOP_SUCCESS;
			case 8: return Commands.STOP_FAIL;
			case 9: return Commands.SHUTDOWN;
			case 10: return Commands.SHUTDOWN_SUCCESS;
			case 11: return Commands.SHUTDOWN_FAIL;
			case 12: return Commands.LOCK_WRITE;
			case 13: return Commands.LOCK_WRITE_SUCCESS;
			case 14: return Commands.LOCK_WRITE_FAIL;
			case 15: return Commands.UNLOCK_WRITE;
			case 16: return Commands.UNLOCK_WRITE_SUCCESS;
			case 17: return Commands.UNLOCK_WRITE_FAIL;
			case 18: return Commands.MOVE_DATA;
			case 19: return Commands.MOVE_DATA_REPLICATE;
			case 20: return Commands.MOVE_DATA_SUCCESS;
			case 21: return Commands.MOVE_DATA_FAIL;
			case 22: return Commands.UPDATE;
			case 23: return Commands.UPDATE_SUCCESS;
			case 24: return Commands.UPDATE_FAIL;
			case 25: return Commands.REPLICATE;
			case 26: return Commands.REPLICATE_SUCCESS;
			case 27: return Commands.PING;
			case 28: return Commands.ECHO;
			default:
				return Commands.UNKNOWN;
		}
	}
	

}
