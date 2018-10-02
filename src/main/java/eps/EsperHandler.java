package eps;

import thriftgen.*;
import thriftgen.Process;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;

import eps.listener.CEPListener;
import eps.listener.CEPListenerToBeSampled;
import eps.listener.CEPTailSamplingListener;
import eventsocket.Event;
import eventsocket.Metric;

public class EsperHandler {
	
	protected static EPRuntime cepRT;
	protected static EPAdministrator cepAdm;

	public static Set<String> traceIdHexSampling = ConcurrentHashMap.newKeySet();
	
	private static final String retentionTime = "1min";
	
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
	    
	    // We register metrics and events as objects the engine will have to handle
	    cepConfig.addEventType("Metric", Metric.class.getName());
	    cepConfig.addEventType("Event", Event.class.getName());
	 
	    // We setup the engine
	    EPServiceProvider cep = EPServiceProviderManager.getProvider("myCEPEngine", cepConfig);
	    
	    cepRT = cep.getEPRuntime();
	    cepAdm = cep.getEPAdministrator();
	    
	    //TRACES WINDOW (traceIdHex)
	    cepAdm.createEPL("create window TracesWindow#unique(traceIdHex)#time(" + retentionTime + ") (traceIdHex string)");
	    cepAdm.createEPL("insert into TracesWindow(traceIdHex)"
	    		+ " select collector.JsonDeserialize.traceIdToHex(traceIdHigh, traceIdLow)"
	    		+ " from Span");
	    
	    //PROCESSES TABLE (hashProcess PK, process)
	    cepAdm.createEPL("create table ProcessesTable (hashProcess int primary key, process thriftgen.Process)");
	    cepAdm.createEPL("on Batch b merge ProcessesTable p"
	    		+ " where collector.JsonDeserialize.hashProcess(b.process) = p.hashProcess"
	    		+ " when not matched then insert select collector.JsonDeserialize.hashProcess(process) as hashProcess, process");
	    
	    //SPANS WINDOW (span, hashProcess, serviceName)
	    cepAdm.createEPL("create window SpansWindow#time(" + retentionTime + ") as (span thriftgen.Span, hashProcess int, serviceName string)");
	    cepAdm.createEPL("insert into SpansWindow"
	    		+ " select s as span, collector.JsonDeserialize.hashProcess(p) as hashProcess, p.serviceName as serviceName"
	    		+ " from Batch[select process as p, * from spans as s]");
	    
	    //ERROR LOG reporter
	    EPStatement cepStatementErrorLogs = cepAdm.createEPL("select Long.toHexString(spanId) as spanId, f.* from " +
	    " Span[select spanId, * from logs][select * from fields as f where f.key=\"error\"]"); 
	    cepStatementErrorLogs.addListener(new CEPListener("Error: "));
	    
	    //DEPENDENCIES WINDOW (traceIdHexFrom, spanIdFrom, traceIdHexTo, spanIdTo)
	    cepAdm.createEPL("create window DependenciesWindow#time(" + retentionTime + ") (traceIdHexFrom string,"
	    		+ " spanIdFrom long, traceIdHexTo string, spanIdTo long)");
	    cepAdm.createEPL("insert into DependenciesWindow"
	    		+ " select collector.JsonDeserialize.traceIdToHex(s.span.traceIdHigh, s.span.traceIdLow) as traceIdHexTo,"
	    		+ " s.span.spanId as spanIdTo,"
	    		+ " collector.JsonDeserialize.traceIdToHex(s.r.traceIdHigh, s.r.traceIdLow) as traceIdHexFrom,"
	    		+ " s.r.spanId as spanIdFrom"
	    		+ " from SpansWindow[select span.spanId, span.traceIdLow, span.traceIdHigh,* from span.references as r] s");
	    
	    //TAIL SAMPLING
	    EPStatement cepStatementTailSampling = cepAdm.createEPL("select rstream * from SpansWindow");
	    cepStatementTailSampling.addListener(new CEPTailSamplingListener());
	   
	    cepAdm.createEPL("create table MeanDurationPerOperation (serviceName string primary key, operationName string primary key,"
	    		+ " meanDuration avg(long), stdDevDuration stddev(long))");
	    cepAdm.createEPL("into table MeanDurationPerOperation select avg(span.duration) as meanDuration,"
	    		+ " stddev(span.duration) as stdDevDuration"
	    		+ " from SpansWindow"
	    		+ " group by serviceName, span.operationName");

	    EPStatement cepStatementOperationDuration = cepAdm.createEPL("select collector.JsonDeserialize.traceIdToHex(span.traceIdHigh, span.traceIdLow)"
	    		+ " as traceId, Long.toHexString(span.spanId) as spanId, span.operationName as operation, span.duration as duration,"
	    		+ " (MeanDurationPerOperation[serviceName, span.operationName].meanDuration + 4 * MeanDurationPerOperation[serviceName, span.operationName].stdDevDuration) as stdDurationPlus4MeanDev"
	    		+ " from SpansWindow"
	    		+ " where span.duration > (MeanDurationPerOperation[serviceName, span.operationName].meanDuration + 4 * MeanDurationPerOperation[serviceName, span.operationName].stdDevDuration)");
	    cepStatementOperationDuration.addListener(new CEPListenerToBeSampled("Long latency than average + 4* std deviation"));
	    
	    //EVENTS listener
	    EPStatement cepEvents = cepAdm.createEPL("select * from Event"); 
	    cepEvents.addListener(new CEPListener("Event: "));
	    
	    //METRICS listener
	    EPStatement cepMetrics = cepAdm.createEPL("select * from Metric"); 
	    cepMetrics.addListener(new CEPListener("Metric: "));
	    
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
	
	public static void sendMetric(Metric metric) {
		
		cepRT.sendEvent(metric);
		
	}
	
	public static void sendEvent(Event event) {

		cepRT.sendEvent(event);
		
	}

}
