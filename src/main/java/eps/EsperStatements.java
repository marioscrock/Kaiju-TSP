package eps;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPStatement;

import eps.listener.CEPListener;
import eps.listener.CEPListenerHighLatencies;
import eps.listener.CEPTailSamplingListener;

public class EsperStatements {
	
	private final static Logger log = LoggerFactory.getLogger(EsperStatements.class);
	
	/**
	 * Define anomalies events.
	 * @param cepAdm {@link com.espertech.esper.client.EPAdministrator EPAdministrator} of the Esper engine.
	 */
	public static void defineAnomalyEvents(EPAdministrator cepAdm) {
		cepAdm.createEPL("create schema Anomaly()");
	    cepAdm.createEPL("create schema TraceAnomaly(traceId string) inherits Anomaly");
	    cepAdm.createEPL("create schema HighLatency3SigmaRule(serviceName string, operationName string,"
	    		+ " spanId string, duration long, startTime long, hostname string) inherits TraceAnomaly");	
	    cepAdm.createEPL("create schema ProcessAnomaly(hashProcess int) inherits Anomaly");
		cepAdm.createEPL("create schema ProcessCPUHigherThan80(serviceName string,"
		    		+ " hostname string, usagePercent float) inherits ProcessAnomaly");
	}
	
	/**
	 * Define system events.
	 * @param cepAdm {@link com.espertech.esper.client.EPAdministrator EPAdministrator} of the Esper engine.
	 */
	public static void defineSystemEvents(EPAdministrator cepAdm) {
		cepAdm.createEPL("create schema SystemEvent(timestamp long)");
	    cepAdm.createEPL("create schema CommitEvent(commit string, commitMsg string) inherits SystemEvent");	
	}
	
	/**
	 * Define a named window storing traceId of incoming spans {@code (traceIdHex string)}.
	 * @param cepAdm {@link com.espertech.esper.client.EPAdministrator EPAdministrator} of the Esper engine.
	 * @param retentionTime Sliding time of the window.
	 */
	public static void defineTracesWindow(EPAdministrator cepAdm, String retentionTime) {
		cepAdm.createEPL("create window TracesWindow#unique(traceIdHex)#time(" + retentionTime + ") (traceIdHex string)");
	    cepAdm.createEPL("insert into TracesWindow(traceIdHex)"
	    		+ " select websocket.JsonLDSerialize.traceIdToHex(traceIdHigh, traceIdLow)"
	    		+ " from Span");		
	}

	/**
	 * Define a table storing processes of incoming batches {@code (hashProcess int primary key, process thriftgen.Process, hostname string)}.
	 * @param cepAdm {@link com.espertech.esper.client.EPAdministrator EPAdministrator} of the Esper engine.
	 */
	public static void defineProcessesTable(EPAdministrator cepAdm) {
		cepAdm.createEPL("create table ProcessesTable (hashProcess int primary key, process thriftgen.Process, hostname string)");
	    cepAdm.createEPL("on Batch b merge ProcessesTable p"
	    		+ " where websocket.JsonLDSerialize.hashProcess(b.process) = p.hashProcess"
	    		+ " when not matched then insert select websocket.JsonLDSerialize.hashProcess(process) as hashProcess, process,"
	    		+ " process.tags.firstOf(t => t.key = 'hostname').getVStr() as hostname");	
	}
	
	/**
	 * Define a named window storing incoming spans and the related process {@code (span thriftgen.Span, hashProcess int, serviceName string)}.
	 * @param cepAdm {@link com.espertech.esper.client.EPAdministrator EPAdministrator} of the Esper engine.
	 * @param retentionTime Sliding time of the window.
	 */
	public static void defineSpansWindow(EPAdministrator cepAdm, String retentionTime) {
		cepAdm.createEPL("create window SpansWindow#time(" + retentionTime + ") as (span thriftgen.Span, hashProcess int, serviceName string)");
		cepAdm.createEPL("insert into SpansWindow"
		    		+ " select s as span, websocket.JsonLDSerialize.hashProcess(p) as hashProcess, p.serviceName as serviceName"
		    		+ " from Batch[select process as p, * from spans as s]");	 
		
	}
	
	/**
	 * Define a named window storing dependencies of incoming spans {@code (traceIdHexFrom string, spanIdFrom long, traceIdHexTo string, spanIdTo long)}
	 * @param cepAdm {@link com.espertech.esper.client.EPAdministrator EPAdministrator} of the Esper engine.
	 * @param retentionTime Sliding time of the window.
	 */
	public static void defineDependenciesWindow(EPAdministrator cepAdm, String retentionTime) {
		cepAdm.createEPL("create window DependenciesWindow#time(" + retentionTime + ") (traceIdHexFrom string,"
	    		+ " spanIdFrom long, traceIdHexTo string, spanIdTo long)");
	    cepAdm.createEPL("insert into DependenciesWindow"
	    		+ " select websocket.JsonLDSerialize.traceIdToHex(s.span.traceIdHigh, s.span.traceIdLow) as traceIdHexTo,"
	    		+ " s.span.spanId as spanIdTo,"
	    		+ " websocket.JsonLDSerialize.traceIdToHex(s.r.traceIdHigh, s.r.traceIdLow) as traceIdHexFrom,"
	    		+ " s.r.spanId as spanIdFrom"
	    		+ " from SpansWindow[select span.spanId, span.traceIdLow, span.traceIdHigh,* from span.references as r] s");	
	}
	
	/**
	 * Define a table storing the mean duration and Welford's Online algorithm coefficients per each operation 
	 * {@code (serviceName string primary key, operationName string primary key, meanDuration double, m2 double, counter long)}
	 * @param cepAdm {@link com.espertech.esper.client.EPAdministrator EPAdministrator} of the Esper engine.
	 */
	public static void defineMeanDurationPerOperationTable(EPAdministrator cepAdm) {
		
		cepAdm.createEPL("create table MeanDurationPerOperation (serviceName string primary key, operationName string primary key,"
	    		+ " meanDuration double, m2 double, counter long)");
	    cepAdm.createEPL("on SpansWindow s"
	    		+ " merge MeanDurationPerOperation m"
	    		+ " where s.serviceName = m.serviceName and s.span.operationName = m.operationName"
	    		+ " when matched"
	    		+ " then update set counter = (initial.counter + 1), "
	    		+ " meanDuration = (initial.meanDuration + ((span.duration - initial.meanDuration)/counter)),"
	    		+ " m2 = (initial.m2 + (span.duration - meanDuration)*(span.duration - initial.meanDuration))"
	    		+ " when not matched"
	    		+ " then insert select s.serviceName as serviceName, s.span.operationName as operationName,"
	    		+ " s.span.duration as meanDuration, 0 as m2, 1 as counter");
	    
	}
	
	/**
	 * Define a table storing the mean duration and Welford's Online algorithm coefficients per each operation 
	 * {@code (serviceName string primary key, operationName string primary key, meanDuration double, m2 double, counter long)}.
	 * It resets the counter of each row when the value exceeds {@code resetValueInt}.
	 * @param cepAdm {@link com.espertech.esper.client.EPAdministrator EPAdministrator} of the Esper engine.
	 * @param resetValueInt The maximum value for the counter. If higher, reset the counter.
	 */
	public static void defineMeanDurationPerOperationTableResetCounter(EPAdministrator cepAdm, String resetValueInt) {
	    
		cepAdm.createEPL("create table MeanDurationPerOperation (serviceName string primary key, operationName string primary key,"
	    		+ " meanDuration double, m2 double, counter long)");
	    cepAdm.createEPL("on SpansWindow s"
	    		+ " merge MeanDurationPerOperation m"
	    		+ " where s.serviceName = m.serviceName and s.span.operationName = m.operationName"
	    		+ " when matched and counter <= " + resetValueInt
	    		+ " then update set counter = (initial.counter + 1), "
	    		+ " meanDuration = (initial.meanDuration + ((span.duration - initial.meanDuration)/counter)),"
	    		+ " m2 = (initial.m2 + (span.duration - meanDuration)*(span.duration - initial.meanDuration))"
	       		+ " when matched and counter > " + resetValueInt
	    		+ " then update set counter = 1,"
	    		+ " meanDuration = s.span.duration,"
	    		+ " m2 = 0"
	    		+ " when not matched"
	    		+ " then insert select s.serviceName as serviceName, s.span.operationName as operationName,"
	    		+ " s.span.duration as meanDuration, 0 as m2, 1 as counter");

	}
	
	/**
	 * Define a named window storing traceIds of traces to be saved {@code (traceId string)} and all the rules 
	 * to populate the window.
	 * @param cepAdm {@link com.espertech.esper.client.EPAdministrator EPAdministrator} of the Esper engine.
	 * @param retentionTime Sliding time of the window.
	 */
	public static void defineTracesToBeSampledWindow(EPAdministrator cepAdm, String retentionTime) {
		cepAdm.createEPL("create window TracesToBeSampledWindow#unique(traceId)#time(" + retentionTime + ") (traceId string)");
	    cepAdm.createEPL("on TraceAnomaly a"
	    		+ " merge TracesToBeSampledWindow t"
	    		+ " where a.traceId = t.traceId"
	    		+ " when not matched"
	    		+ " then insert into TracesToBeSampledWindow select a.traceId as traceId");
	}

	
	/**
	 * Register a {@link eps.listener.CEPListener CEPListener} reporting the average duration of requests grouped by
	 * customer.
	 * @param cepAdm {@link com.espertech.esper.client.EPAdministrator EPAdministrator} of the Esper engine.
	 */
	public static void perCustomerDuration(EPAdministrator cepAdm) {
		 EPStatement perCustomerDuration = cepAdm.createEPL("select customerId, avg(duration) as meanDuration, stddev(duration) as stdDevDuration"
		    		+ " from Span(parentSpanId = 0)"
		    		+ " [select duration, l.getFields().firstOf(f => f.key ='customer_id').getVStr() as customerId" 
		    		+ " from logs as l where l.fields.anyOf(f => f.key='customer_id')]"
		    		+ " group by customerId"
		    		+ " output snapshot every 5 seconds"
		    		+ " order by meanDuration desc");
		perCustomerDuration.addListener(new CEPListener("LatenciesCalcPerCustomerId: "));
	}
	
	/**
	 * Register a {@link eps.listener.CEPListener CEPListener} reporting time CPU RouteCalc grouped by CustomerId 
	 * (Contained Events syntax).
	 * @param cepAdm {@link com.espertech.esper.client.EPAdministrator EPAdministrator} of the Esper engine.
	 * @param retentionTime Sliding time of the window.
	 */
	public static void resourceUsageCustomerCE(EPAdministrator cepAdm, String retentionTime) {
		EPStatement resourceUsageCustomerCE = cepAdm.createEPL("select customerId,"
	    		+ " sum(time) as timeCPURouteCalcperCustomerId"
	    		+ " from Span(operationName = \"HTTP GET /customer\")"
	    		+ "[select traceIdHigh, traceIdLow, l.getFields().firstOf(f => f.key ='customer_id').getVStr() as customerId"
	    		+ " from logs as l where l.fields.anyOf(f => f.key='customer_id')]"
	    		+ "#time(" + retentionTime + ") as s1,"
	    		+ " Span(operationName = \"HTTP GET /route\")"
	    		+ "[select traceIdHigh, traceIdLow, l.getFields().firstOf(f => f.key ='time').getVDouble() as time"
	    		+ " from logs as l where l.fields.anyOf(f => f.getVStr() ='RouteCalc')]"
	    		+ "#time(" + retentionTime + ") as s2"
	    		+ " where s1.traceIdHigh = s2.traceIdHigh and s1.traceIdLow = s2.traceIdLow"
	    		+ " group by customerId"
	    		+ " output last every 10 seconds");//+ " output every " + retentionTime + "");
		resourceUsageCustomerCE.addListener(new CEPListener("TimeCPURouteCalcperCustomerId: "));	
	}
	
	/**
	 * Register a {@link eps.listener.CEPListener CEPListener} reporting time CPU RouteCalc grouped by CustomerId 
	 * (Collections syntax).
	 * @param cepAdm {@link com.espertech.esper.client.EPAdministrator EPAdministrator} of the Esper engine.
	 * @param retentionTime Sliding time of the window.
	 */
	public static void resourceUsageCustomer(EPAdministrator cepAdm, String retentionTime) {
		 EPStatement resourceUsageCustomer = cepAdm.createEPL("select "
		    		+ " s1.logs.firstOf(l => l.fields.anyOf(f => f.key ='customer_id')).getFields().firstOf(f => f.key ='customer_id').getVStr() as customerId,"
		    		+ " sum(s2.logs.firstOf(l => l.fields.anyOf(f => f.getVStr() ='RouteCalc')).getFields().firstOf(f => f.key ='time').getVDouble()) as timeCPURouteCalcperCustomerId"
		    		+ " from Span(operationName = \"HTTP GET /customer\")#time(" + retentionTime + ") as s1,"
		    		+ " Span(operationName = \"HTTP GET /route\")#time(" + retentionTime + ") as s2"
		    		+ " where s1.traceIdHigh = s2.traceIdHigh and s1.traceIdLow = s2.traceIdLow"
		    		+ " and s1.logs.anyOf(l => l.fields.anyOf(f => f.key ='customer_id'))"
		    		+ " and s2.logs.anyOf(l => l.fields.anyOf(f => f.getVStr() ='RouteCalc'))"
		    		+ " group by s1.logs.firstOf(l => l.fields.anyOf(f => f.key ='customer_id')).getFields().firstOf(f => f.key ='customer_id').getVStr()"
		    		+ " output last every 10 seconds");
		 resourceUsageCustomer.addListener(new CEPListener("TimeCPURouteCalcperCustomerId: "));	
	}
	
	/**
	 * Register a {@link eps.listener.CEPListener CEPListener} reporting time CPU RouteCalc grouped by SessionId 
	 * (Contained Events syntax).
	 * @param cepAdm {@link com.espertech.esper.client.EPAdministrator EPAdministrator} of the Esper engine.
	 * @param retentionTime Sliding time of the window.
	 */
	public static void resourceUsageSessionCE(EPAdministrator cepAdm, String retentionTime) {
		EPStatement resourceUsageSessionCE = cepAdm.createEPL("select sessionId,"
	    		+ " sum(time) as timeCPURouteCalcperSessionId"
	    		+ " from Span(operationName = \"Driver::findNearest\")"
	    		+ "[select traceIdHigh, traceIdLow, l.getFields().firstOf(f => f.key='value').getVStr() as sessionId"
	    		+ " from logs as l where l.fields.anyOf(f => f.getVStr()='session')]"
	    		+ "#time(" + retentionTime + ") as s1,"
	    		+ " Span(operationName = \"HTTP GET /route\")"
	    		+ "[select traceIdHigh, traceIdLow, l.getFields().firstOf(f => f.key ='time').getVDouble() as time"
	    		+ " from logs as l where l.fields.anyOf(f => f.getVStr()='RouteCalc')]"
	    		+ "#time(" + retentionTime + ") as s2"
	    		+ " where s1.traceIdHigh = s2.traceIdHigh and s1.traceIdLow = s2.traceIdLow"
	    		+ " group by sessionId"
	    		+ " output last every 10 seconds");//+ " output every " + retentionTime + "");
		resourceUsageSessionCE.addListener(new CEPListener("TimeCPURouteCalcperSessionId: "));	
	}
	
	/**
	 * Register a {@link eps.listener.CEPListener CEPListener} reporting time CPU RouteCalc grouped by SessionId 
	 * (Collections syntax).
	 * @param cepAdm {@link com.espertech.esper.client.EPAdministrator EPAdministrator} of the Esper engine.
	 * @param retentionTime Sliding time of the window.
	 */
	public static void resourceUsageSession(EPAdministrator cepAdm, String retentionTime) {
		EPStatement resourceUsageSession = cepAdm.createEPL("select "
	    		+ " s1.logs.firstOf(l => l.fields.anyOf(f => f.getVStr() ='session')).getFields().firstOf(f => f.key ='value').getVStr() as sessionId,"
	    		+ " sum(s2.logs.firstOf(l => l.fields.anyOf(f => f.getVStr() ='RouteCalc')).getFields().firstOf(f => f.key ='time').getVDouble()) as timeCPURouteCalcperSessionId"
	    		+ " from Span(operationName = \"Driver::findNearest\")#time(" + retentionTime + ") as s1,"
	    		+ " Span(operationName = \"HTTP GET /route\")#time(" + retentionTime + ") as s2"
	    		+ " where s1.traceIdHigh = s2.traceIdHigh and s1.traceIdLow = s2.traceIdLow"
	    		+ " and s1.logs.anyOf(l => l.fields.anyOf(f => f.getVStr() ='session'))"
	    		+ " and s2.logs.anyOf(l => l.fields.anyOf(f => f.getVStr() ='RouteCalc'))"
	    		+ " group by s1.logs.firstOf(l => l.fields.anyOf(f => f.getVStr() ='session')).getFields().firstOf(f => f.key ='value').getVStr()"
	    		+ " output last every 10 seconds");//+ " output every " + retentionTime + "");
		resourceUsageSession.addListener(new CEPListener("TimeCPURouteCalcperSessionId: "));
	}
	
	/**
	 * Register a {@link eps.listener.CEPListener CEPListener} and generate a {@code HighLatency3SigmaRule} event
	 * reporting spans such that operation (duration - meanDuration) > 3*stdDev.  
	 * @param cepAdm {@link com.espertech.esper.client.EPAdministrator EPAdministrator} of the Esper engine.
	 */
	public static void highLatencies(EPAdministrator cepAdm) {
		 EPStatement highLatencies =cepAdm.createEPL(""
		    		+ " insert into HighLatency3SigmaRule"
		    		+ " select websocket.JsonLDSerialize.traceIdToHex(span.traceIdHigh, span.traceIdLow) as traceId,"
		    		+ " Long.toHexString(span.spanId) as spanId, serviceName, span.operationName as operationName,"
		    		+ " span.startTime as startTime, span.duration as duration, p.hostname as hostname"
		    		+ " from SpansWindow as s join ProcessesTable as p"
		    		+ " where s.hashProcess = p.hashProcess and (span.duration - MeanDurationPerOperation[serviceName, span.operationName].meanDuration) >"
		    		+ " 3 * java.lang.Math.sqrt((MeanDurationPerOperation[serviceName, span.operationName].m2) / (MeanDurationPerOperation[serviceName, span.operationName].counter))");
		 highLatencies.addListener(new CEPListener("(duration - meanDuration) > 3*stdDev: "));		
	}
	
	/**
	 * Register a {@link eps.listener.CEPListenerHighLatencies CEPListenerHighLatencies} saving 
	 * {@code HighLatency3SigmaRule} events to file.
	 * @param cepAdm {@link com.espertech.esper.client.EPAdministrator EPAdministrator} of the Esper engine.
	 * @param filepath Filepath of the file to save events.
	 */
	public static void reportHighLatencies(EPAdministrator cepAdm, String filepath) {
		EPStatement reportHighLatencies = cepAdm.createEPL("select * from HighLatency3SigmaRule");
	    reportHighLatencies.addListener(new CEPListenerHighLatencies(filepath));	
	}
	
	/**
	 * Register a {@link eps.listener.CEPTailSamplingListener CEPTailSamplingListener} saving
	 * spans exiting from {@code SpansWindow} to file.
	 * @param cepAdm {@link com.espertech.esper.client.EPAdministrator EPAdministrator} of the Esper engine.
	 * @param filepath Filepath of the file to save spans.
	 */
	public static void tailSampling(EPAdministrator cepAdm, String filepath) {
	    EPStatement tailSampling = cepAdm.createEPL("select rstream * from SpansWindow as s"
	    		+ " where exists (select * from TracesToBeSampledWindow "
	    		+ "where traceId = (websocket.JsonLDSerialize.traceIdToHex(s.span.traceIdHigh, s.span.traceIdLow)))");
	    tailSampling.addListener(new CEPTailSamplingListener(filepath)); 		
	}
	
	/**
	 * Generate detected {@code CommitEvent} from Event stream.
	 * @param cepAdm {@link com.espertech.esper.client.EPAdministrator EPAdministrator} of the Esper engine.
	 */
	public static void insertCommitEvents(EPAdministrator cepAdm) {
		  cepAdm.createEPL("insert into CommitEvent"
		    		+ " select timestamp, context('commit_id') as commit,"
		    		+ " payload('commit_msg') as commitMsg"
		    		+ " from Event"
		    		+ " where context.containsKey('commit_id')"); 		
	}
	
	/**
	 * Register a {@link eps.listener.CEPListener CEPListener} reporting all {@code SystemEvent}.
	 * @param cepAdm {@link com.espertech.esper.client.EPAdministrator EPAdministrator} of the Esper engine.
	 */
	public static void systemEvents(EPAdministrator cepAdm) {
	    EPStatement cepSystemEvents = cepAdm.createEPL("select * from SystemEvent");
	    cepSystemEvents.addListener(new CEPListener("SystemEvent: "));	
	}
	
	/**
	 * Detect pattern {@code CommitEvent} followed by {@code Anomaly} events within {@code within} parameter.
	 * @param cepAdm {@link com.espertech.esper.client.EPAdministrator EPAdministrator} of the Esper engine.
	 * @param within String representing within time for pattern detection.
	 */
	public static void anomalyAfterCommit(EPAdministrator cepAdm, String within) {
		EPStatement anomalyAfterCommit = cepAdm.createEPL("select b.commit, a.* from pattern [" 
				  + "b=CommitEvent -> every a=Anomaly where timer:within(" + within +")]"); 	
		anomalyAfterCommit.addListener(new CEPListener("Anomaly after Commit: "));
	}
	    
	/**
	 * Generate detected {@code ProcessCPUHigherThan80} from Metric stream.
	 * @param cepAdm {@link com.espertech.esper.client.EPAdministrator EPAdministrator} of the Esper engine.
	 */
	public static void insertProcessCPUHigherThan80(EPAdministrator cepAdm) {
	    cepAdm.createEPL("insert into ProcessCPUHigherThan80"
	    		+ " select hashProcess, process.serviceName as serviceName, hostname, Float.parseFloat(fields('usage_percent')) as usagePercent"
	    		+ " from Metric(name='docker_container_cpu') as m join ProcessesTable as p"
	    		+ " where m.tags('host') =  p.hostname and"
	    		+ " Float.parseFloat(fields('usage_percent')) > 80.0"
	    		+ " output last every 10sec");
	}
	
	/**
	 * Detect pattern {@code HighLatency3SigmaRule} and {@code ProcessCPUHigherThan80} in same host within {@code within} parameter.
	 * @param cepAdm {@link com.espertech.esper.client.EPAdministrator EPAdministrator} of the Esper engine.
	 * @param within String representing within time for pattern detection.
	 */
	public static void highCPUandHighLatencySameHost(EPAdministrator cepAdm, String within) {
		EPStatement anomalyAfterCommit = cepAdm.createEPL("select a.hostname as hostname from pattern [" 
				  + "a=ProcessCPUHigherThan80 and b=HighLatency3SigmaRule(hostname = a.hostname) where timer:within(" + within + ")]"); 	
		anomalyAfterCommit.addListener(new CEPListener("HighLatency3SigmaRule and ProcessCPUHigherThan80 same host: "));
	}
	
	/**
	 * Register a {@link eps.listener.CEPListener CEPListener} reporting all {@code Metric} event and {@code Event} event.
	 * @param cepAdm {@link com.espertech.esper.client.EPAdministrator EPAdministrator} of the Esper engine.
	 */
	public static void debugStatements(EPAdministrator cepAdm) {
		//METRICS listener
	    EPStatement cepMetrics = cepAdm.createEPL("select * from Metric"); 
	    cepMetrics.addListener(new CEPListener("Metric: "));
	    
	    //EVENTS listener
	    EPStatement cepEvents = cepAdm.createEPL("select * from Event"); 
	    cepEvents.addListener(new CEPListener("Event: "));	
	}

	public static void defaultStatements(EPAdministrator cepAdm, String retentionTime) {
		
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
		
	}

	public static void parseStatements(EPAdministrator cepAdm, String retentionTime) {
		
		List<String> replaced = new ArrayList<String>();
		
		try (Stream<String> lines = Files.lines(Paths.get("./stmts/statements.txt"))) {
			   replaced = lines
			       .map(line -> line.replaceAll(":retentionTime:", retentionTime))
			       .collect(Collectors.toList());
		} catch (IOException e) {
			log.error("Error in reading statements from file: " + e.getMessage());
		}
		
		try {
			for (String s : replaced) {
				String[] s_array = s.split("=", 2);
				
				String[] prefix = s_array[0].split(",");
				for(String key : prefix) {
					//TODO
					switch (key) {
					case "value":	
						break;
					default:
						break;
					}
				}	
				
				String s_stmt = s_array[1];
				EPStatement stmt = cepAdm.createEPL(s_stmt);
				stmt.addListener(new CEPListener(""));
			}	
		    		    
		} catch (Exception e) {
			log.error("Failed validating statements file: " + e.getMessage());
		}
		
	}

}
