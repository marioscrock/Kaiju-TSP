package eventsocket;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Class representing a generic event.
 * @author Mario
 *
 */
public class Event implements Serializable {

	private static final long serialVersionUID = -2601014412189297417L;
	
	public Long timestamp;
	public Map<String, String> payload = new HashMap<>();
	public Map<String, String> context = new HashMap<>();
	
	public Event (Long timestamp, Map<String, String> payload, Map<String, String> context) {
		this.timestamp = timestamp;
		this.payload = payload;
		this.context = context;
	}
	
	/**
	 * Get the event payload {@code Map<String, String>}.
	 * @return The event payload {@code Map<String, String>}.
	 */
	public Map<String, String> getPayload() {
		return payload;
	}
	
	/**
	 * Set the event payload {@code Map<String, String>}
	 * @param event The event payload {@code Map<String, String>}
	 */
	public void setPayload(Map<String, String> payload) {
		this.payload = payload;
	}
	
	/**
	 * Get the event context {@code Map<String, String>}.
	 * @return The event context {@code Map<String, String>}.
	 */
	public Map<String, String> getContext() {
		return context;
	}
	
	/**
	 * Set the event context {@code Map<String, String>}
	 * @param event The event context {@code Map<String, String>}
	 */
	public void setContext(Map<String, String> context) {
		this.context = context;
	}
	
	/**
	 * Get the timestamp of the event.
	 * @return The timestamp of the event.
	 */
	public Long getTimestamp() {
		return timestamp;
	}
	
	/**
	 * Set the timestamp of the event.
	 * @param timestamp The timestamp of the event.
	 */
	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}
	
	@Override
	public String toString() {
		return "Event [timestamp=" + timestamp + ", payload=" + payload + ", context=" + context + "]";
	}

}
