package eps;

import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPStatement;

import eps.listener.CEPListener;
import eps.listener.CEPListenerHighLatencies;
import eps.listener.CEPTailSamplingListener;

public class EsperStatements {
	
	/**
	 * Define Anomaly events.
	 * @param cepAdm
	 */
	public static void defineAnomalyEvents(EPAdministrator cepAdm) {
		cepAdm.createEPL("create schema Anomaly()");
	    cepAdm.createEPL("create schema TraceAnomaly(traceId string) inherits Anomaly");
	    cepAdm.createEPL("create schema HighLatency3SigmaRule(serviceName string, operationName string,"
	    		+ " spanId string, duration long, startTime long, hostname string) inherits TraceAnomaly");	
	}

	public static void defineSystemEvents(EPAdministrator cepAdm) {
		cepAdm.createEPL("create schema SystemEvent(timestamp long)");
	    cepAdm.createEPL("create schema CommitEvent(commit string, commitMsg string) inherits SystemEvent");	
	}

	public static void defineTracesWindow(EPAdministrator cepAdm, String retentionTime) {
		cepAdm.createEPL("create window TracesWindow#unique(traceIdHex)#time(" + retentionTime + ") (traceIdHex string)");
	    cepAdm.createEPL("insert into TracesWindow(traceIdHex)"
	    		+ " select collector.JsonLDSerialize.traceIdToHex(traceIdHigh, traceIdLow)"
	    		+ " from Span");		
	}

	public static void defineProcessesTable(EPAdministrator cepAdm) {
		cepAdm.createEPL("create table ProcessesTable (hashProcess int primary key, process thriftgen.Process, hostname string)");
	    cepAdm.createEPL("on Batch b merge ProcessesTable p"
	    		+ " where collector.JsonLDSerialize.hashProcess(b.process) = p.hashProcess"
	    		+ " when not matched then insert select collector.JsonLDSerialize.hashProcess(process) as hashProcess, process,"
	    		+ " process.tags.firstOf(t => t.key = 'hostname').getVStr() as hostname");	
	}

	public static void defineSpansWindow(EPAdministrator cepAdm, String retentionTime) {
		cepAdm.createEPL("create window SpansWindow#time(" + retentionTime + ") as (span thriftgen.Span, hashProcess int, serviceName string)");
		cepAdm.createEPL("insert into SpansWindow"
		    		+ " select s as span, collector.JsonLDSerialize.hashProcess(p) as hashProcess, p.serviceName as serviceName"
		    		+ " from Batch[select process as p, * from spans as s]");	 
		
	}
	
	public static void defineDependenciesWindow(EPAdministrator cepAdm, String retentionTime) {
		cepAdm.createEPL("create window DependenciesWindow#time(" + retentionTime + ") (traceIdHexFrom string,"
	    		+ " spanIdFrom long, traceIdHexTo string, spanIdTo long)");
	    cepAdm.createEPL("insert into DependenciesWindow"
	    		+ " select collector.JsonLDSerialize.traceIdToHex(s.span.traceIdHigh, s.span.traceIdLow) as traceIdHexTo,"
	    		+ " s.span.spanId as spanIdTo,"
	    		+ " collector.JsonLDSerialize.traceIdToHex(s.r.traceIdHigh, s.r.traceIdLow) as traceIdHexFrom,"
	    		+ " s.r.spanId as spanIdFrom"
	    		+ " from SpansWindow[select span.spanId, span.traceIdLow, span.traceIdHigh,* from span.references as r] s");	
	}
	
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
	
	public static void defineTracesToBeSampledWindow(EPAdministrator cepAdm, String retentionTime) {
		cepAdm.createEPL("create window TracesToBeSampledWindow#unique(traceId)#time(" + retentionTime + ") (traceId string)");
	    cepAdm.createEPL("on TraceAnomaly a"
	    		+ " merge TracesToBeSampledWindow t"
	    		+ " where a.traceId = t.traceId"
	    		+ " when not matched"
	    		+ " then insert into TracesToBeSampledWindow select a.traceId as traceId");
	}

	public static void gaugeRequestsPerHostname(EPAdministrator cepAdm) {
		EPStatement gaugeRequestsPerHostname = cepAdm.createEPL("select hostname, count(*)"
	    		+ "from Batch[select process.tags.firstOf(t => t.key = 'hostname').getVStr() as hostname, * from spans as s where s.parentSpanId = 0]"
	    		+ "group by hostname");
	    gaugeRequestsPerHostname.addListener(new CEPListener("Gauge per hostname: "));	
	}
	
	/**
	 * Reports all (spanId, fields) for each log with "error" as key
	 * @param cepAdm
	 */
	public static void errorLogs(EPAdministrator cepAdm) {
		EPStatement errorLogs = cepAdm.createEPL("select Long.toHexString(spanId) as spanId, f.* from " +
			    " Span[select spanId, * from logs][select * from fields as f where f.key=\"error\"]"); 
		errorLogs.addListener(new CEPListener("Error: "));
	}

	public static void topKOperationDuration(EPAdministrator cepAdm, String K) {
		EPStatement tableDuration = cepAdm.createEPL("select serviceName, operationName, meanDuration, (m2/counter) as variance, counter"
	    		+ " from MeanDurationPerOperation"
	    		+ " output snapshot every 5 seconds"
	    		+ " order by meanDuration desc"
	    		+ " limit " + K);
	    tableDuration.addListener(new CEPListener("Top-" + K + " operation duration: "));
		
	}

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

	public static void highLatencies(EPAdministrator cepAdm) {
		 EPStatement highLatencies =cepAdm.createEPL(""
		    		+ " insert into HighLatency3SigmaRule"
		    		+ " select collector.JsonLDSerialize.traceIdToHex(span.traceIdHigh, span.traceIdLow) as traceId,"
		    		+ " Long.toHexString(span.spanId) as spanId, serviceName, span.operationName as operationName,"
		    		+ " span.startTime as startTime, span.duration as duration, p.hostname as hostname"
		    		+ " from SpansWindow as s join ProcessesTable as p"
		    		+ " where s.hashProcess = p.hashProcess and (span.duration - MeanDurationPerOperation[serviceName, span.operationName].meanDuration) >"
		    		+ " 3 * java.lang.Math.sqrt((MeanDurationPerOperation[serviceName, span.operationName].m2) / (MeanDurationPerOperation[serviceName, span.operationName].counter))");
		 highLatencies.addListener(new CEPListener("(duration - meanDuration) > 3*stdDev: "));		
	}

	public static void reportHighLatencies(EPAdministrator cepAdm, String filepath) {
		EPStatement reportHighLatencies = cepAdm.createEPL("select * from HighLatency3SigmaRule");
	    reportHighLatencies.addListener(new CEPListenerHighLatencies(filepath));	
	}

	public static void tailSampling(EPAdministrator cepAdm, String filepath) {
	    EPStatement tailSampling = cepAdm.createEPL("select rstream * from SpansWindow as s"
	    		+ " where exists (select * from TracesToBeSampledWindow "
	    		+ "where traceId = (collector.JsonLDSerialize.traceIdToHex(s.span.traceIdHigh, s.span.traceIdLow)))");
	    tailSampling.addListener(new CEPTailSamplingListener(filepath)); 		
	}

	public static void insertCommitEvents(EPAdministrator cepAdm) {
		  cepAdm.createEPL("insert into CommitEvent"
		    		+ " select java.time.Instant.now().toEpochMilli() as timestamp, event('commit') as commit,"
		    		+ " event('commitMsg') as commitMsg"
		    		+ " from Event"
		    		+ " where event('type') = 'CommitEvent'"); 		
	}

	public static void systemEvents(EPAdministrator cepAdm) {
	    EPStatement cepSystemEvents = cepAdm.createEPL("select * from SystemEvent");
	    cepSystemEvents.addListener(new CEPListener("SystemEvent: "));	
	}

	public static void debugStatements(EPAdministrator cepAdm) {
		//METRICS listener
	    EPStatement cepMetrics = cepAdm.createEPL("select * from Metric"); 
	    cepMetrics.addListener(new CEPListener("Metric: "));
	    
	    //EVENTS listener
	    EPStatement cepEvents = cepAdm.createEPL("select * from Event"); 
	    cepEvents.addListener(new CEPListener("Event: "));	
	}

}
