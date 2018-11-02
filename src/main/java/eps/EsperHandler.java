package eps;

import thriftgen.*;
import thriftgen.Process;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;

import eventsocket.Event;
import eventsocket.Metric;

public class EsperHandler {
	
	protected static EPRuntime cepRT;
	protected static EPAdministrator cepAdm;

	public static String retentionTime = "2min";
	
	public static void initializeHandler() {
		
		//The Configuration is meant only as an initialization-time object.
	    Configuration cepConfig = new Configuration();
	    
	    //Basic EVENTS
	    addEventTypes(cepConfig);
	 
	    // We setup the engine
	    EPServiceProvider cep = EPServiceProviderManager.getProvider("myCEPEngine", cepConfig);
	    
	    cepRT = cep.getEPRuntime();
	    cepAdm = cep.getEPAdministrator();
	    
	    //Additional EVENTS
	    EsperStatements.defineAnomalyEvents(cepAdm);
	    EsperStatements.defineSystemEvents(cepAdm);
	    
	    //TABLES and NAMED WINDOWS
	    //TRACES WINDOW (traceId PK)
	    EsperStatements.defineTracesWindow(cepAdm, retentionTime);
	    //PROCESSES TABLE (hashProcess PK, process)
	    EsperStatements.defineProcessesTable(cepAdm);
	    //SPANS WINDOW (span, hashProcess, serviceName)
	    EsperStatements.defineSpansWindow(cepAdm, retentionTime);
	    //DEPENDENCIES WINDOW (traceIdHexFrom, spanIdFrom, traceIdHexTo, spanIdTo)
	    EsperStatements.defineDependenciesWindow(cepAdm, retentionTime);
	    
	    //MEAN DURATION PER OPERATION TABLE
	    //Welford's Online algorithm to compute running mean and variance
	    EsperStatements.defineMeanDurationPerOperationTable(cepAdm);
	    
	    //TRACES TO BE SAMPLED
	    EsperStatements.defineTracesToBeSampledWindow(cepAdm, retentionTime);
	    
	    //STATEMENTS
//	    EsperStatements.gaugeRequestsPerHostname(cepAdm);
//	    EsperStatements.errorLogs(cepAdm);
	    
//	    EsperStatements.topKOperationDuration(cepAdm, "10");
//	    EsperStatements.perCustomerDuration(cepAdm);
	    
	    //RESOURCE USAGE ATTRIBUTION
	    //CE -> Contained Event Selection
//	    EsperStatements.resourceUsageCustomerCE(cepAdm, retentionTime);
//	    EsperStatements.resourceUsageCustomer(cepAdm, retentionTime);
//	    EsperStatements.resourceUsageSessionCE(cepAdm, retentionTime);
//	    EsperStatements.resourceUsageSession(cepAdm, retentionTime);
	    
	    //ANOMALIES DETECTION
	    //Three-sigma rule to detech anomalies (info https://en.wikipedia.org/wiki/68–95–99.7_rule)
//	    EsperStatements.highLatencies(cepAdm);
//	    EsperStatements.reportHighLatencies(cepAdm);
	    
	    //TAIL SAMPLING
//	    EsperStatements.tailSampling(cepAdm);   
	    
	    //EVENTS
//	    EsperStatements.insertCommitEvents(cepAdm);   
//	    EsperStatements.systemEvents(cepAdm);
	    
	    //DEBUG socket
//	    EsperStatements.debugStatements(cepAdm);
	    
	    //START API
	    KaijuAPI.threadAPI();
	    
	}
	
	private static void addEventTypes(Configuration cepConfig) {
		
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