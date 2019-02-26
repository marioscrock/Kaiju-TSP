package eventsocket;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

/**
 * Class representing a structured log.
 * @author Mario
 *
 */
public class FLog implements Serializable {

	private static final long serialVersionUID = 709484586326437353L;
	public Map<String, String> fields = new HashMap<String, String>();	
	
	public FLog(Map<String, JsonElement> fields) {
	
		Gson gson = new Gson();
		
		for (Entry<String,JsonElement> e : fields.entrySet()) 	
			this.fields.put(e.getKey(), gson.toJson(e.getValue()));
		
	}
	
	public Map<String, String> getFields() {
		return fields;
	}
	
	public void setFields(Map<String, String> fields) {
		this.fields = fields;
	}
	
	@Override
	public String toString() {
		return "FLog [fields=" + fields + "]";
	}
	
}