package eps;

import java.time.Instant;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

import collector.TimingCollector;

public class CEPSpansTimingListener implements UpdateListener {
	
	private TimingCollector spansTimingCollector = new TimingCollector("/spansTiming.csv");
	
	@Override
	public void update(EventBean[] newData, EventBean[] oldData) {
		
		for (EventBean e : newData) {
			
			//traceId, spanId, startTime, duration, eventTime
			String[] timing = new String[5];
			timing[4] = Long.toString(Instant.now().toEpochMilli());
			timing[0] = (String) e.get("traceId");
			timing[1] = (String) e.get("spanId");
			timing[2] = Long.toString((Long) e.get("startTime"));
			timing[3] = Long.toString((Long) e.get("duration"));
			spansTimingCollector.addRecord(timing);
			
		}
		
	}
	

}
