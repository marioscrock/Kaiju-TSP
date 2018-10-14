package eps.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

import collector.RecordCollector;
import eps.EventToJsonConverter;

public class CEPTailSamplingListener implements UpdateListener {
	
	private final static Logger log = LoggerFactory.getLogger(CEPTailSamplingListener.class);
	private RecordCollector sampledCollector = new RecordCollector("./sampled.txt", 0);
	
	@Override
	public void update(EventBean[] newData, EventBean[] oldData) {
		
		try {
            if (newData.length > 0) {
            	
            	for (EventBean e : newData) {
            		String[] record = new String[1];
            		record[0] = EventToJsonConverter.spanFromEB(e);
            		sampledCollector.addRecord(record);
            		//log.info("Span  sampled");
            	}
            } 
        } catch (Exception e) {
			log.info(e.getStackTrace().toString());
			log.info(e.getMessage());
		}
		
	}
	

}
