package eventsocket;

import java.util.HashMap;
import java.util.Map;

/**
 * Class representing a generic event as a {@code Map<String, String>}.
 * @author Mario
 *
 */
public class Event {
	
	public Map<String, String> event = new HashMap<>();
	
	/**
	 * Get the event {@code Map<String, String>}.
	 * @return The event {@code Map<String, String>}.
	 */
	public Map<String, String> getEvent() {
		return event;
	}
	
	/**
	 * Set the event {@code Map<String, String>}
	 * @param event The event {@code Map<String, String>}
	 */
	public void setEvent(Map<String, String> event) {
		this.event = event;
	}
	
	@Override
	public String toString() {
		return "Event [event=" + event + "]";
	}

}
