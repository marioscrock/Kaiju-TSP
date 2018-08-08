package eps;

import thriftgen.*;
import thriftgen.Process;

import java.util.HashMap;
import java.util.Map;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;

import collector.JsonDeserialize;

public class EsperHandler {
	
	protected static EPRuntime cepRT;
	
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
	    
	    Map<String, Object> spanProcess = new HashMap<String, Object>();
	    spanProcess.put("processHash", int.class);
	    spanProcess.put("traceId", String.class);
	    spanProcess.put("spanId", long.class);
	    spanProcess.put("serviceName", String.class);
	    cepConfig.addEventType("SpanProcess", spanProcess);
	 
	    // We setup the engine
	    EPServiceProvider cep = EPServiceProviderManager.getProvider("myCEPEngine", cepConfig);
	    
	    cepRT = cep.getEPRuntime();
	    EPAdministrator cepAdm = cep.getEPAdministrator();
	    
	    cepAdm.createEPL("create window TracesWindow#unique(traceIdHex) (traceIdHex string)");
	    cepAdm.createEPL("insert into TracesWindow(traceIdHex) select collector.JsonDeserialize.traceIdToHex(traceIdHigh, traceIdLow) from Span");
	    
	    cepAdm.createEPL("create table Processes (hashProcess int primary key, process thriftgen.Process)");
	    cepAdm.createEPL("on Batch b merge Processes p where collector.JsonDeserialize.hashProcess(b.process) = p.hashProcess "
	    		+ "when not matched then insert select collector.JsonDeserialize.hashProcess(process) as hashProcess, process");
	    
	    cepAdm.createEPL("create window SpansWindow#time(1 min) as (span thriftgen.Span, hashProcess int, serviceName string)");
	    cepAdm.createEPL("insert into SpansWindow select s as span, sp.processHash as hashProcess, sp.serviceName as serviceName from Span#time(2 min) as s, SpanProcess#time(2 min) as sp where s.spanId = sp.spanId");
	    
	    //SAMPLES Contained-Event Selection
	    
	    cepAdm.createEPL("create window LogsFields#time(1 min) as (spanId string, field thriftgen.Tag, timestamp long)");
	    cepAdm.createEPL("insert into LogsFields select Long.toHexString(spanId) as spanId, f as field, timestamp as timestamp from " +
        " Span[select spanId, timestamp from logs][select * from fields as f]"); 
	    EPStatement cepStatementFields = cepAdm.createEPL("select count(*) from LogsFields as f where f.field.VStr=\"redis timeout\""); 
	    cepStatementFields.addListener(new CEPListener("# Fields redis with window"));
	 
	    EPStatement cepStatementFields2 = cepAdm.createEPL("select operationName from " +
	    " Span[select operationName, * from logs][select * from fields as f where f.VStr=\"redis timeout\"]"); 
	    cepStatementFields2.addListener(new CEPListener("# Fields"));
	    
	    EPStatement cepStatementLogs = cepAdm.createEPL("select count(*) from " +
                "Span[logs]");
	    cepStatementLogs.addListener(new CEPListener("# Logs"));
	    
	    // We register an EPL statement
	    EPStatement cepStatementSpans = cepAdm.createEPL("select collector.JsonDeserialize.traceIdToHex(traceIdHigh, traceIdLow) as traceId, Long.toHexString(spanId) as spanId, startTime, duration "
	    		+ "from Span");
	    cepStatementSpans.addListener(new CEPSpansListener());
	    
	    cepAdm.createEPL("create context Trace partition by traceIdHigh and traceIdLow "
				+ "from Span( operationName != \"HTTP GET /metrics\" )"
				+ "terminated after 5 sec");
	    
	    EPStatement cepStatement = cepAdm.createEPL("context Trace select collector.JsonDeserialize.traceIdToHex(traceIdHigh, traceIdLow) "
	    		+ "as traceId, count(*) from Span output last when terminated");
	    cepStatement.addListener(new CEPListener("#Span"));
	    
	    EPStatement cepStatement2 = cepAdm.createEPL("select count(*) from " +
                "Batch");
	    cepStatement2.addListener(new CEPListener("#Batches"));
	   
	
	    cepAdm.createEPL("create table MeanDurationPerOperation (operationName string primary key, meanDuration avg(long))");
	    cepAdm.createEPL("into table MeanDurationPerOperation select avg(duration) as meanDuration from Span"
	    		+ " group by operationName");

	    EPStatement cepStatement3 = cepAdm.createEPL("context Trace select collector.JsonDeserialize.traceIdToHex(traceIdHigh, traceIdLow) "
	    		+ "as traceId, operationName, duration, MeanDurationPerOperation[operationName].meanDuration as meanDuration from Span(duration > MeanDurationPerOperation[operationName].meanDuration)");
	    cepStatement3.addListener(new CEPListener("Long latency than average"));
	    
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
			
			int processHash = JsonDeserialize.hashProcess(batch.getProcess());
			String serviceName = batch.getProcess().getServiceName();
			
			for(Span span : batch.getSpans()) {
				
				cepRT.sendEvent(span);
				
				// Create a SpanProcess event mapping span and processes
				Map<String, Object> event = new HashMap<String, Object>();
				event.put("processHash", processHash);
				event.put("spanId", span.getSpanId());
				event.put("traceId", JsonDeserialize.traceIdToHex(span.getTraceIdHigh(), span.getTraceIdLow()));
				event.put("serviceName", serviceName);
				cepRT.sendEvent(event, "SpanProcess");
				
			}
		}
	}

}
