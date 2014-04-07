package common.messages;

import java.util.List;
import metadata.MetaData;
public class KVMessageImpl implements KVMessage {
	private String mKey;
	private String mValue;
	private StatusType mStatusType;
	private List<MetaData> metadata;
	
	public KVMessageImpl() {
	}
	
	public KVMessageImpl(String key,String value,StatusType statusType) {
		mKey=key;
		mValue=value;
		mStatusType=statusType;
	}
	
	public KVMessageImpl(String key,String value,StatusType statusType, List<MetaData> metadata) {
		mKey=key;
		mValue=value;
		mStatusType=statusType;
		this.metadata = metadata;
	}

	@Override
	public String getKey() {
		return mKey;
	}
	
	@Override
	public List<MetaData> getMetaData() {
		return metadata;
	}
	
	@Override
	public String getValue() {
		return mValue;
	}
	
	@Override
	public StatusType getStatus() {
		return mStatusType;
	}

	public void setKey(String key){
		mKey=key;
	}
	
	public void setMetadata(List<MetaData> metadata) {
		this.metadata = metadata;
	}
	
	public void setValue(String value){
		mValue=value;
	}

	public void setStatus(StatusType statusType){
		mStatusType=statusType;
	}
	
	public static StatusType getStatusType(int status){
		
		switch(status){
			case 0: return StatusType.GET;
			case 1: return StatusType.GET_ERROR;
			case 2: return StatusType.GET_SUCCESS;
			case 3: return StatusType.PUT;
			case 4: return StatusType.PUT_SUCCESS;
			case 5: return StatusType.PUT_UPDATE;
			case 6: return StatusType.PUT_ERROR;
			case 7: return StatusType.DELETE_SUCCESS;
			case 8: return StatusType.DELETE_ERROR;
			case 9: return StatusType.SERVER_STOPPED;
			case 10: return StatusType.SERVER_WRITE_LOCK;
			case 11: return StatusType.SERVER_NOT_RESPONSIBLE;
			case 12: return StatusType.REPLICA_PUT;
			case 13: return StatusType.REPLICA_PUT_SUCCESS;
			case 14: return StatusType.REPLICA_PUT_UPDATE;
			case 15: return StatusType.REPLICA_PUT_ERROR; 
			case 16: return StatusType.REPLICA_DELETE_SUCCESS;
			case 17: return StatusType.REPLICA_DELETE_ERROR;
			case 18: return StatusType.DELETE_TOPOLOGICAL;
			default:
				return StatusType.UNKNOWN;
		}
	}

}
