package eventsocket;

import java.util.HashMap;
import java.util.Map;

/**
 * Class representing a metric following the InfluxDB specification.
 * @see <a href="https://docs.influxdata.com/telegraf/v1.8/data_formats/output/json/>
 * @author Mario
 *
 */
public class Metric {
	
	public String name;
	public Map<String, String> fields = new HashMap<String, String>();
	public Long timestamp;
	public Map<String, String> tags = new HashMap<String, String>();
	
	
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
