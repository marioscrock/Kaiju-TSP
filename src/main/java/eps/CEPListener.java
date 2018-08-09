package eps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

public class CEPListener implements UpdateListener {
	
	private final static Logger log = LoggerFactory.getLogger(CEPListener.class);
	private String name;
	
	public CEPListener (String name) {
		this.name = name;
	}
	
	@Override
	public void update(EventBean[] newData, EventBean[] oldData) {
		
		StringBuilder sb = new StringBuilder();
		
		if (newData != null) {
			for (EventBean e : newData)
				sb.append(e.getUnderlying() + "\n");
		
			log.info("Event " + name + " " + sb.toString());
		}
		
	}
	

}
