package eps;

import thriftgen.*;
import thriftgen.Process;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;

import eps.listener.CEPListener;
import eps.listener.CEPListenerAnomalies;
import eps.listener.CEPTailSamplingListener;
import eventsocket.Event;
import eventsocket.Metric;

public class EsperHandler {
	
	protected static EPRuntime cepRT;
	protected static EPAdministrator cepAdm;

	public static String retentionTime = "2min";
	
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
	    
	    //DEFINE EVENTS
	    cepAdm.createEPL("create schema Anomaly(traceId string)");
	    cepAdm.createEPL("create schema HighLatency3SigmaRule(serviceName string, operationName string,"
	    		+ " spanId string, duration long, startTime long, hostname string) inherits Anomaly");
	    
	    cepAdm.createEPL("create schema SystemEvent(timestamp long)");
	    cepAdm.createEPL("create schema CommitEvent(commit string, commitMsg string) inherits SystemEvent");
	    
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
	    //cepStatementErrorLogs.addListener(new CEPListener("Error: "));
	    
	    //DEPENDENCIES WINDOW (traceIdHexFrom, spanIdFrom, traceIdHexTo, spanIdTo)
	    cepAdm.createEPL("create window DependenciesWindow#time(" + retentionTime + ") (traceIdHexFrom string,"
	    		+ " spanIdFrom long, traceIdHexTo string, spanIdTo long)");
	    cepAdm.createEPL("insert into DependenciesWindow"
	    		+ " select collector.JsonDeserialize.traceIdToHex(s.span.traceIdHigh, s.span.traceIdLow) as traceIdHexTo,"
	    		+ " s.span.spanId as spanIdTo,"
	    		+ " collector.JsonDeserialize.traceIdToHex(s.r.traceIdHigh, s.r.traceIdLow) as traceIdHexFrom,"
	    		+ " s.r.spanId as spanIdFrom"
	    		+ " from SpansWindow[select span.spanId, span.traceIdLow, span.traceIdHigh,* from span.references as r] s");
	   
	    cepAdm.createEPL("create table MeanDurationPerOperation (serviceName string primary key, operationName string primary key,"
	    		+ " meanDuration avg(long), stdDevDuration stddev(long))");
	    cepAdm.createEPL("into table MeanDurationPerOperation select avg(span.duration) as meanDuration,"
	    		+ " stddev(span.duration) as stdDevDuration"
	    		+ " from SpansWindow"
	    		+ " group by serviceName, span.operationName");
	    EPStatement tableDuration= cepAdm.createEPL("select serviceName, operationName, meanDuration, stdDevDuration"
	    		+ " from MeanDurationPerOperation"
	    		+ " output snapshot every 5 seconds"
	    		+ " order by meanDuration desc");
	    //tableDuration.addListener(new CEPListener("Latencies: "));
	    
	    cepAdm.createEPL("create table TracesToBeSampled (traceId string primary key)");
	    cepAdm.createEPL("on Anomaly a"
	    		+ " merge TracesToBeSampled t"
	    		+ " where a.traceId = t.traceId"
	    		+ " when not matched"
	    		+ " then insert into TracesToBeSampled select a.traceId as traceId");
	    
	    //Three-sigma rule to detech anomalies (info https://en.wikipedia.org/wiki/68–95–99.7_rule)
	    EPStatement cepStatementOperationDuration = cepAdm.createEPL(""
	    		+ " insert into HighLatency3SigmaRule"
	    		+ " select collector.JsonDeserialize.traceIdToHex(span.traceIdHigh, span.traceIdLow) as traceId,"
	    		+ " Long.toHexString(span.spanId) as spanId, serviceName, span.operationName as operationName,"
	    		+ " span.startTime as startTime, span.duration as duration,"
	    		+ " process.tags.firstOf(t => t.key = \"hostname\").getVStr() as hostname"
	    		+ " from SpansWindow as s join ProcessesTable as p"
	    		+ " where s.hashProcess = p.hashProcess and java.lang.Math.abs(span.duration - MeanDurationPerOperation[serviceName, span.operationName].meanDuration) > (3 * MeanDurationPerOperation[serviceName, span.operationName].stdDevDuration)");
	    cepStatementOperationDuration.addListener(new CEPListenerAnomalies());
	    
	    //TAIL SAMPLING
	    EPStatement cepStatementTailSampling = cepAdm.createEPL("select rstream * from SpansWindow as s where exists (select * from TracesToBeSampled where traceId = (collector.JsonDeserialize.traceIdToHex(s.span.traceIdHigh, s.span.traceIdLow)))");
	    cepStatementTailSampling.addListener(new CEPTailSamplingListener());    
	    
	    //EVENTS listener
	    EPStatement cepEvents = cepAdm.createEPL("select * from Event"); 
	    cepEvents.addListener(new CEPListener("Event: "));
	    
	    cepAdm.createEPL("insert into CommitEvent"
	    		+ " select java.time.Instant.now().toEpochMilli() as timestamp, cast(event.get(\"commit\")?, string) as commit,"
	    		+ " cast(event.get(\"commitMsg\")?, string) as commitMsg"
	    		+ " from Event"
	    		+ " where event.get(\"type\") = \"CommitEvent\""); 
	    
	    EPStatement cepSystemEvents = cepAdm.createEPL("select * from SystemEvent");
	    cepSystemEvents.addListener(new CEPListener("SystemEvent: "));
	    
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
