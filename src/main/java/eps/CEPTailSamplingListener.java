package eps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

import collector.JsonDeserialize;
import collector.RecordCollector;
import thriftgen.Span;

public class CEPTailSamplingListener implements UpdateListener {
	
	private final static Logger log = LoggerFactory.getLogger(CEPTailSamplingListener.class);
	private RecordCollector sampledCollector = new RecordCollector("./sampled.txt", 0);
	
	@Override
	public void update(EventBean[] newData, EventBean[] oldData) {
		
		try {
            if (newData.length > 0) {
            	
            	for (EventBean e : newData) {
            		
                	Span s = (Span) e.get("span");
                	String traceIdHex = JsonDeserialize.traceIdToHex(s.getTraceIdHigh(), s.getTraceIdLow());
                	if (EsperHandler.traceIdHexSampling.contains(traceIdHex)) {
                		
                		String[] record = new String[1];
                		record[0] = KaijuAPI.spanFromEB(e);
                		sampledCollector.addRecord(record);
                		log.info("Trace " + traceIdHex + " sampled");
                		
                	}
            	}
            } 
        } catch (Exception e) {
			log.info(e.getStackTrace().toString());
			log.info(e.getMessage());
		}
		
	}
	

}
