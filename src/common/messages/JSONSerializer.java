package common.messages;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;



import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import metadata.MetaData;
import common.messages.KVAdminMessage.Commands;
import common.messages.KVMessage.StatusType;

/**
 * Class for JSON Marshaling and Unmarshaling of messages.
 */
public class JSONSerializer {

	/**
	 * Method for marshaling key-value message.
	 * 
	 * @param key 
	 * 		Key to marshal
	 * @param value 
	 * 		Value associated with key
	 * @param status 
	 * 		Status type for this message
	 * @return 
	 * 		Returns Marshaled TextMessage.
	 */
	public static TextMessage marshal(String key, String value,
			StatusType status) {
		KVMessageImpl msg = new KVMessageImpl(key, value, status);

		JsonObjectBuilder objectBuilder = Json.createObjectBuilder();
		JsonObject object = objectBuilder.add("key", msg.getKey())
				.add("value", msg.getValue())
				.add("status", msg.getStatus().ordinal()).build();

		return new TextMessage(object.toString());
	}

	/**
	 * Method for Marshaling status messages.
	 * 
	 * @param status 
	 * 			Message Status to be marshaled.
	 * @return 
	 * 		Returns Marshaled TextMessage.
	 */
	public static TextMessage marshal(StatusType status){	
		KVMessageImpl msg = new KVMessageImpl();
		msg.setStatus(status);

		JsonObjectBuilder objectBuilder = Json.createObjectBuilder();		
		JsonObject object = objectBuilder.add
				("topologicalDelete", msg.getStatus().ordinal()).build();

		return new TextMessage(object.toString());
	}

	/**
	 * Method to send Marshaled metadata to the client.
	 * 
	 * @param msg
	 * 		Message to be marshaled.
	 * @return 
	 * 		Marshaled TextMessage.
	 */
	public static TextMessage marshal(KVMessage msg) {
		JsonObjectBuilder objectBuilder = Json.createObjectBuilder()
				.add("key", msg.getKey())
				.add("value", msg.getValue())
				.add("status", msg.getStatus().ordinal());

		if (msg.getMetaData() != null) {
			JsonArrayBuilder array = Json.createArrayBuilder();
			List<MetaData> list = msg.getMetaData();
			for (MetaData metadataItem : list) {
				array.add(Json.createObjectBuilder()
						.add("ip", metadataItem.getIP())
						.add("port", metadataItem.getPort())
						.add("start", metadataItem.getRangeStart())
						.add("end", metadataItem.getRangeEnd()));
			}
			objectBuilder.add("metadata", array);
		}

		JsonObject value = objectBuilder.build();
		return new TextMessage(value.toString());
	}

	/**
	 * Method for unmarshaling TextMessage.
	 * 
	 * @param txtMsg
	 * 		Message to be unmarshaled.
	 * @return
	 * 		UnMarshalled KVMessage.
	 */
	public static KVMessageImpl unMarshal(TextMessage txtMsg) {
		String strMsg = txtMsg.getMsg();
		JsonObject jsonObject = Json.createReader(new StringReader(strMsg))
				.readObject();

		List<MetaData> metaDatas = new ArrayList<MetaData>();
		JsonArray jarray = jsonObject.getJsonArray("metadata");	

		if(jarray!=null){
			for(int i=0;i<jarray.size();i++){
				MetaData meta;
				meta = new MetaData(jarray.getJsonObject(i).getString("ip") 
						,jarray.getJsonObject(i).getString("port") 
						,jarray.getJsonObject(i).getString("start") 
						,jarray.getJsonObject(i).getString("end"));

				metaDatas.add(meta);
			}
		}

		return new KVMessageImpl(jsonObject.getString("key"),
				jsonObject.getString("value"),
				KVMessageImpl.getStatusType(jsonObject.getInt("status")),
				metaDatas);
	}

	/**
	 * Method to Marshal message plus metadata to the client.
	 * 
	 * @param kvmsg
	 * 		KVMessage to be marshaled.
	 * @return 
	 * 		Return JSON string corresponding to the message.
	 */
	public static String Marshal(KVMessage kvmsg) {
		KVMessageImpl kvmsgimpl = (KVMessageImpl) kvmsg;
		JsonObjectBuilder objectBuilder = Json.createObjectBuilder()
				.add("key", kvmsgimpl.getKey())
				.add("value", kvmsgimpl.getValue())
				.add("status", kvmsgimpl.getStatus().ordinal());

		JsonArrayBuilder array = Json.createArrayBuilder();		
		if (kvmsgimpl.getMetaData() != null) {
			List<MetaData> list = kvmsgimpl.getMetaData();
			for (MetaData metadataItem : list) {
				array.add(Json.createObjectBuilder().add("ip", metadataItem.getIP())
						.add("port", metadataItem.getPort())
						.add("start", metadataItem.getRangeStart())
						.add("end", metadataItem.getRangeEnd()));
			}

		}
		objectBuilder.add("metadata", array);

		return objectBuilder.build().toString();
	}

	/**
	 * Method to marshal KVAdmin Messages.
	 * 
	 * @param command
	 * 		Command to be associated with KVAdmin message.
	 * @return
	 * 		Returns Marshaled TextMessage.	
	 */
	public static TextMessage marshalKVAdminMsg(Commands command){
		JsonObjectBuilder builder = Json.createObjectBuilder()
				.add("command", command.ordinal());
		JsonObject value = builder.build();

		return new TextMessage(value.toString());
	}

	/**
	 * Method to unmarshal KVAdmin Messages specified with Command param.
	 * @param txtMsg
	 * 		TextMessage to be unmarshaled.
	 * @return
	 * 		Returns unmarshaled KVAdminMessage.
	 */
	public static KVAdminMessageImpl unmarshalKVAdminMsgForCommand(TextMessage txtMsg){
		String strMsg = txtMsg.getMsg();		
		JsonObject jsonObject = Json.createReader(new StringReader(strMsg))
				.readObject();

		KVAdminMessageImpl kvAdminMessage= new KVAdminMessageImpl();
		kvAdminMessage.setCommand(KVAdminMessageImpl.getCommandType(jsonObject.getInt("command")));

		return kvAdminMessage;
	}

	/**
	 * Method to marshal KVAdminMessage.
	 * @param list
	 * 		MetaData list of all Nodes.
	 * @param command
	 * 		Command associated with this message.
	 * @param destination
	 * 		Destination Node ip:port information.
	 * @param range
	 * 		Range in the ring, corresponding to Data that is to be moved.
	 * @return
	 * 		Marshaled TextMessage.
	 */
	public static TextMessage marshalKVAdminMsg(List<MetaData> list, Commands command, String destination, String range ){
		JsonObjectBuilder builder = Json.createObjectBuilder()
				.add("adminMsg", true)
				.add("command", command.ordinal())
				.add("range", range)
				.add("destination",destination);

		if(list!=null){
			JsonArrayBuilder array = Json.createArrayBuilder();
			for(MetaData item:list){
				array.add(Json.createObjectBuilder()
						.add("ip", item.getIP())
						.add("port", item.getPort())
						.add("start", item.getRangeStart())
						.add("end", item.getRangeEnd()));
			}
			builder.add("metadata", array);
		}

		JsonObject value = builder.build();
		return new TextMessage(value.toString());
	}

	/**
	 * Method for unmarshaling KVAdminMessage having MetaData Information.
	 * @param txtMsg
	 * 		TextMessage to be unmarshaled.
	 * @return
	 * 		UnMarshaled KVAdminMessage.
	 */
	public static KVAdminMessageImpl unmarshalKVAdminMsg(TextMessage txtMsg){
		String strMsg = txtMsg.getMsg();
		JsonObject jsonObject = Json.createReader(new StringReader(strMsg))
				.readObject();		

		List<MetaData> metaDatas = new ArrayList<MetaData>();
		JsonArray jarray = jsonObject.getJsonArray("metadata");	

		if(jarray!=null){
			for(int i=0;i<jarray.size();i++){
				MetaData meta;

				meta = new MetaData(jarray.getJsonObject(i).getString("ip") 
						,jarray.getJsonObject(i).getString("port") 
						,jarray.getJsonObject(i).getString("start") 
						,jarray.getJsonObject(i).getString("end"));

				metaDatas.add(meta);
			}
		}

		return new KVAdminMessageImpl(metaDatas
				,KVAdminMessageImpl.getCommandType(jsonObject.getInt("command"))
				,jsonObject.getString("range"),jsonObject.getString("destination"));
	}
}
