package eventsocket;

import java.util.HashMap;
import java.util.Map;

public class Event {
	
	public Map<String, String> event = new HashMap<>();

	public Map<String, String> getEvent() {
		return event;
	}

	public void setEvent(Map<String, String> event) {
		this.event = event;
	}
	
	@Override
	public String toString() {
		return "Event [event=" + event + "]";
	}

}
