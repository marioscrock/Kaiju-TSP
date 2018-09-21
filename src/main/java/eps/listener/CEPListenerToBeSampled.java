package eps.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

import eps.EsperHandler;

public class CEPListenerToBeSampled implements UpdateListener {
	
	private final static Logger log = LoggerFactory.getLogger(CEPListenerToBeSampled.class);
	private String name;
	
	public CEPListenerToBeSampled (String name) {
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
		
		try {
            if (newData.length > 0)	
            	for (EventBean e : newData) {
            		EsperHandler.traceIdHexSampling.add((String) e.get("traceId"));
            	}
        } catch (Exception e) {
			log.info(e.getStackTrace().toString());
			log.info(e.getMessage());
		}
		
	}
	

}
