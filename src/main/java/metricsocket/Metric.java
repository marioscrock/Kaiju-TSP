package metricsocket;

import java.util.HashMap;
import java.util.Map;

public class Metric {
	
	public Map<String, String> fields = new HashMap<String, String>();
	public String name;
	
	public Map<String, String> tags = new HashMap<String, String>();
	public Long timestamp;
	
	public Map<String, String> getFields() {
		return fields;
	}
	public void setFields(Map<String, String> fields) {
		this.fields = fields;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Map<String, String> getTags() {
		return tags;
	}
	public void setTags(Map<String, String> tags) {
		this.tags = tags;
	}
	public Long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}
	
	@Override
	public String toString() {
		return "Metric [fields=" + fields + ", name=" + name + ", tags=" + tags + ", timestamp=" + timestamp + "]";
	}

}
