package eps;

import thriftgen.*;
import thriftgen.Process;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;

public class EsperHandler {
	
	protected static EPRuntime cepRT;
	private static final String retentionTime = "2min";
	
	public static void initializeHandler() {
		
		//The Configuration is meant only as an initialization-time object.
	    Configuration cepConfig = new Configuration();
	    
	    // We register thriftgen classes as objects the engine will have to handle
	    cepConfig.addEventType("Batch", Batch.class.getName());
	    cepConfig.addEventType("Span", Span.class.getName());
	    cepConfig.addEventType("Process", Process.class.getName());
	    cepConfig.addEventType("Log", Log.class.getName());
	    cepConfig.addEventType("Tag", Tag.class.getName());
	    cepConfig.addEventType("SpanRef", SpanRef.class.getName());
	 
	    // We setup the engine
	    EPServiceProvider cep = EPServiceProviderManager.getProvider("myCEPEngine", cepConfig);
	    
	    cepRT = cep.getEPRuntime();
	    EPAdministrator cepAdm = cep.getEPAdministrator();
	    
	    cepAdm.createEPL("create window TracesWindow#unique(traceIdHex)#time(" + retentionTime + ") (traceIdHex string)");
	    cepAdm.createEPL("insert into TracesWindow(traceIdHex)"
	    		+ " select collector.JsonDeserialize.traceIdToHex(traceIdHigh, traceIdLow)"
	    		+ " from Span");
	    
	    cepAdm.createEPL("create table ProcessesTable (hashProcess int primary key, process thriftgen.Process)");
	    cepAdm.createEPL("on Batch b merge ProcessesTable p"
	    		+ " where collector.JsonDeserialize.hashProcess(b.process) = p.hashProcess"
	    		+ " when not matched then insert select collector.JsonDeserialize.hashProcess(process) as hashProcess, process");
	    
	    cepAdm.createEPL("create window SpansWindow#time(" + retentionTime + ") as (span thriftgen.Span, hashProcess int, serviceName string)");
	    cepAdm.createEPL("insert into SpansWindow"
	    		+ " select s as span, collector.JsonDeserialize.hashProcess(p) as hashProcess, p.serviceName as serviceName"
	    		+ " from Batch[select process as p, * from spans as s]");
	 
	    EPStatement cepStatementErrorLogs = cepAdm.createEPL("select Long.toHexString(spanId) as spanId, f.* from " +
	    " Span[select spanId, * from logs][select * from fields as f where f.key=\"error\"]"); 
	    cepStatementErrorLogs.addListener(new CEPListener("Error: "));

	    //TIMING --> REMOVE?
//	    EPStatement cepStatementSpans = cepAdm.createEPL("select collector.JsonDeserialize.traceIdToHex(traceIdHigh, traceIdLow) as traceId, Long.toHexString(spanId) as spanId, startTime, duration "
//	    		+ "from Span");
//	    cepStatementSpans.addListener(new CEPSpansListener());
	    
	    EPStatement cepStatementSpanCount = cepAdm.createEPL("select count(*) from " +
                "Span output last every 10 sec");
	    cepStatementSpanCount.addListener(new CEPListener("#Span"));
	   
	    cepAdm.createEPL("create table MeanDurationPerOperation (serviceName string primary key, operationName string primary key, meanDuration avg(long), stdDevDuration stddev(long))");
	    cepAdm.createEPL("into table MeanDurationPerOperation select avg(span.duration) as meanDuration, stddev(span.duration) as stdDevDuration from SpansWindow"
	    		+ " group by serviceName, span.operationName");

	    EPStatement cepStatement3 = cepAdm.createEPL("select collector.JsonDeserialize.traceIdToHex(span.traceIdHigh, span.traceIdLow)"
	    		+ " as traceId, span.operationName, span.duration,"
	    		+ " (MeanDurationPerOperation[serviceName, span.operationName].meanDuration + 2 * MeanDurationPerOperation[serviceName, span.operationName].stdDevDuration) as stdDurationPlus2MeanDev"
	    		+ " from SpansWindow"
	    		+ " where span.duration > (MeanDurationPerOperation[serviceName, span.operationName].meanDuration + 2 * MeanDurationPerOperation[serviceName, span.operationName].stdDevDuration)");
	    cepStatement3.addListener(new CEPListener("Long latency than average + 2* std deviation"));
	    
	    Thread APIThread = new Thread(new Runnable() {
			
			@Override
			public void run() {
				KaijuAPI.initAPI();	
			}
			
		});
	    
	    APIThread.run();
	    
	}
	
	public static void sendBatch(Batch batch) {
		
		cepRT.sendEvent(batch);
		
		if(batch.getSpans() != null) {
			
			for(Span span : batch.getSpans()) {
				cepRT.sendEvent(span);			
			}
		}
	}

}
