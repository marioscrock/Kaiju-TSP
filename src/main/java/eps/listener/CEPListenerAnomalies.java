package eps.listener;

import java.time.Instant;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

import collector.RecordCollector;

public class CEPListenerAnomalies implements UpdateListener {
	
	private RecordCollector anomaliesCollector;
	
	public CEPListenerAnomalies() {
		anomaliesCollector = new RecordCollector("./anomalies.csv", 0);
	}
	
	@Override
	public void update(EventBean[] newData, EventBean[] oldData) {
		
		if (newData.length > 0) {
         	for (EventBean e : newData) {
         		
         		String[] record = new String[8];
         		record[7] = Long.toString(Instant.now().toEpochMilli());
         		record[0] = (String) e.get("traceId");
         		record[1] = (String) e.get("spanId");
         		record[2] = (String) e.get("operationName");
         		record[3] = (String) e.get("serviceName");
         		record[4] = Long.toString((Long) e.get("startTime"));
         		record[5] = Long.toString((Long) e.get("duration"));
         		record[6] = (String) e.get("hostname");	
         		anomaliesCollector.addRecord(record);
         		
         	}
         } 
		
		

	}

}
