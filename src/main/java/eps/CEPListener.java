package eps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

public class CEPListener implements UpdateListener {
	
	private final static Logger log = LoggerFactory.getLogger(CEPListener.class);
	
	@Override
	public void update(EventBean[] newData, EventBean[] oldData) {
		log.info("Event received: " + newData[0].getUnderlying());
	}
	

}
