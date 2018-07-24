package eps;

import thriftgen.*;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;

public class EsperHandler {
	
	private EPRuntime cepRT;
	
	public void initializeHandler() {
		
		//The Configuration is meant only as an initialization-time object.
	    Configuration cepConfig = new Configuration();
	    
	    // We register thriftgen classes as objects the engine will have to handle
	    cepConfig.addEventType("Batch", Batch.class.getName());
	    cepConfig.addEventType("Span", Span.class.getName());
	 
	    // We setup the engine
	    EPServiceProvider cep = EPServiceProviderManager.getProvider("myCEPEngine",cepConfig);
	    
	    cepRT = cep.getEPRuntime();
	    EPAdministrator cepAdm = cep.getEPAdministrator();
	    
	    // We register an EPL statement
	    EPStatement cepStatementSpans = cepAdm.createEPL("select collector.JsonDeserialize.traceIdToHex(traceIdHigh, traceIdLow) as traceId, Long.toHexString(spanId) as spanId, startTime, duration "
	    		+ "from Span");
	    cepStatementSpans.addListener(new CEPSpansTimingListener());
	    
	    cepAdm.createEPL("create context Trace partition by traceIdHigh and traceIdLow "
				+ "from Span( operationName != \"HTTP GET /metrics\" )"
				+ "terminated after 5 sec");
	    
	    EPStatement cepStatement = cepAdm.createEPL("context Trace select collector.JsonDeserialize.traceIdToHex(traceIdHigh, traceIdLow) "
	    		+ "as traceId, count(*) from Span output last when terminated");
	    cepStatement.addListener(new CEPListener("#Span"));
	    
	    EPStatement cepStatement2 = cepAdm.createEPL("select count(*) from " +
                "Batch");
	    cepStatement2.addListener(new CEPListener("#Batches"));
	   
	 
	    //cepAdm.createEPL("create context Operation partition by operationName from Span");
	    cepAdm.createEPL("create table MeanDurationPerOperation (operationName string primary key, meanDuration avg(long))");
	    cepAdm.createEPL("into table MeanDurationPerOperation select avg(duration) as meanDuration from Span"
	    		+ " group by operationName");

	    EPStatement cepStatement3 = cepAdm.createEPL("context Trace select collector.JsonDeserialize.traceIdToHex(traceIdHigh, traceIdLow) "
	    		+ "as traceId, operationName, duration, MeanDurationPerOperation[operationName].meanDuration as meanDuration from Span(duration > MeanDurationPerOperation[operationName].meanDuration)");
	    cepStatement3.addListener(new CEPListener("Long latency than average"));
	    
	}
	
	public void sendBatch(Batch batch) {
		
		cepRT.sendEvent(batch);
		
		if(batch.getSpans() != null) 		
			for(Span span : batch.getSpans())
				cepRT.sendEvent(span);	
		
	}

}
