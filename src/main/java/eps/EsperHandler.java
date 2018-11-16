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

/**
 * Class to manage the Esper Engine.
 * @author Mario
 */
public class EsperHandler {
	
	public static String retentionTime = "2min";
	
	protected static EPRuntime cepRT;
	protected static EPAdministrator cepAdm;
	
	/**
	 * Static method to initialize the Esper Engine and the selected statements, the Kaiju API.
	 */
	public static void initializeHandler() {
		
		//Check not already initialized
		if(cepRT == null) {
			
			//The Configuration is meant only as an initialization-time object.
		    Configuration cepConfig = new Configuration();
		    
		    /*
		     * Basic EVENTS
		     */
		    addEventTypes(cepConfig);
		 
		    // We setup the engine
		    EPServiceProvider cep = EPServiceProviderManager.getProvider("myCEPEngine", cepConfig);
		    
		    cepRT = cep.getEPRuntime();
		    cepAdm = cep.getEPAdministrator();
		    
		    /*
		     * Additional EVENTS
		     */
		    EsperStatements.defineAnomalyEvents(cepAdm);
		    EsperStatements.defineSystemEvents(cepAdm);
		    
		    /*
		     * TABLES and NAMED WINDOWS
		     */
		    // TRACES WINDOW (traceId PK)
		    EsperStatements.defineTracesWindow(cepAdm, retentionTime);
		    // PROCESSES TABLE (hashProcess PK, process)
		    EsperStatements.defineProcessesTable(cepAdm);
		    // SPANS WINDOW (span, hashProcess, serviceName)
		    EsperStatements.defineSpansWindow(cepAdm, retentionTime);
		    // DEPENDENCIES WINDOW (traceIdHexFrom, spanIdFrom, traceIdHexTo, spanIdTo)
		    EsperStatements.defineDependenciesWindow(cepAdm, retentionTime);
		    
		    // MEAN DURATION PER OPERATION TABLE (serviceName PK, operationName PK, meanDuration, m2, counter)
		    //Welford's Online algorithm to compute running mean and variance
		    EsperStatements.defineMeanDurationPerOperationTable(cepAdm);
	//	    EsperStatements.defineMeanDurationPerOperationTableResetCounter(cepAdm, 1000);
		    
		    //TRACES TO BE SAMPLED WINDOW (traceId)
		    EsperStatements.defineTracesToBeSampledWindow(cepAdm, retentionTime);
		    
		    /*
		     * STATEMENTS
		     */
	//	    EsperStatements.gaugeRequestsPerHostname(cepAdm, retentionTime);
	//	    EsperStatements.errorLogs(cepAdm);
		    
	//	    EsperStatements.topKOperationDuration(cepAdm, "10");
	//	    EsperStatements.perCustomerDuration(cepAdm);
		    
		    // RESOURCE USAGE ATTRIBUTION
		    // CE -> Contained Event Selection
	//	    EsperStatements.resourceUsageCustomerCE(cepAdm, retentionTime);
	//	    EsperStatements.resourceUsageCustomer(cepAdm, retentionTime);
	//	    EsperStatements.resourceUsageSessionCE(cepAdm, retentionTime);
	//	    EsperStatements.resourceUsageSession(cepAdm, retentionTime);
		    
		    // ANOMALIES DETECTION
		    // Three-sigma rule to detect anomalies (info https://en.wikipedia.org/wiki/68–95–99.7_rule)
		    EsperStatements.highLatencies(cepAdm);
		    EsperStatements.reportHighLatencies(cepAdm, "./anomalies.csv");
		    EsperStatements.insertProcessCPUHigherThan80(cepAdm);
		    
		    // TAIL SAMPLING
		    EsperStatements.tailSampling(cepAdm, "./sampled.txt");   
		    
		    //PATTERN
		    EsperStatements.anomalyAfterCommit(cepAdm, "15min");
		    EsperStatements.highCPUandHighLatencySameHost(cepAdm, "10sec");
		    
		    /*
		     * EVENTS
		     */
		    EsperStatements.insertCommitEvents(cepAdm);   
		    EsperStatements.systemEvents(cepAdm);
		    
		    /*
		     * DEBUG socket
		     */
		    EsperStatements.debugStatements(cepAdm);
		    
		    /*
		     * START API
		     */
		    Thread APIThread = new Thread(new Runnable() {
				
				@Override
				public void run() {
					KaijuAPI.initAPI();	
				}
				
			});
		    
		    APIThread.run();
		}
	    
	}
	
	private static void addEventTypes(Configuration cepConfig) {
		
	    // We register thriftgen classes as objects the engine will have to handle
		// JAEGER model
	    cepConfig.addEventType("Batch", Batch.class.getName());
	    cepConfig.addEventType("Span", Span.class.getName());
	    cepConfig.addEventType("Process", Process.class.getName());
	    cepConfig.addEventType("Log", Log.class.getName());
	    cepConfig.addEventType("Tag", Tag.class.getName());
	    cepConfig.addEventType("SpanRef", SpanRef.class.getName());
	   
	    // SOCKET events
	    // Metrics -> JSON influxDB
	    // Events -> Custom definition
	    // We register metrics and events as objects the engine will have to handle
	    cepConfig.addEventType("Metric", Metric.class.getName());
	    cepConfig.addEventType("Event", Event.class.getName());
		
	}

	/**
	 * Static method to send a {@link thriftgen.Batch Batch} event, and a {@link thriftgen.Span Span} for each span 
	 * in the batch to the Esper engine.
	 * @param batch The {@link thriftgen.Batch Batch} to be sent to the Esper engine.
	 */
	public static void sendBatch(Batch batch) {
		
		cepRT.sendEvent(batch);
		
		if(batch.getSpans() != null) {
			
			for(Span span : batch.getSpans()) {
				cepRT.sendEvent(span);			
			}
		}
	}
	
	/**
	 * Static method to send a {@link eventSocket.Metric Metric} event to the Esper engine.
	 * @param metric The {@link eventSocket.Metric Metric} to be sent to the Esper engine.
	 */
	public static void sendMetric(Metric metric) {
		
		cepRT.sendEvent(metric);
		
	}
	
	/**
	 * Static method to send a {@link eventSocket.Event Event} event to the Esper engine.
	 * @param event The {@link eventSocket.Event Event} to be sent to the Esper engine.
	 */
	public static void sendEvent(Event event) {

		cepRT.sendEvent(event);
		
	}

}