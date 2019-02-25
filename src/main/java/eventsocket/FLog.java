package eventsocket;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Class representing a structured log.
 * @author Mario
 *
 */
public class FLog implements Serializable {

	private static final long serialVersionUID = -5972347553924444286L;
	
	public Map<String, String> fields = new HashMap<String, String>();	
	
	public FLog(Map<String, String> fields) {
		
		this.fields.putAll(fields);		
		
		Gson gson = new Gson();
		JsonParser parser = new JsonParser();
		
		for (String k : fields.keySet()) {
			if (isJSONValid(fields.get(k))) {
				
				JsonObject jObj = (JsonObject) parser.parse(fields.get(k));
				@SuppressWarnings("unchecked")
				HashMap<String,Object> otherFields = gson.fromJson(jObj, HashMap.class);
				
				for (Entry<String, Object> f : otherFields.entrySet())
					fields.put(f.getKey(), f.getValue().toString());			
			}
		}	
		
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
	
	public boolean isJSONValid(String test) {
	    try {
	        new JSONObject(test);
	    } catch (JSONException ex) {
	        try {
	            new JSONArray(test);
	        } catch (JSONException ex1) {
	            return false;
	        }
	    }
	    return true;
	}
	
}