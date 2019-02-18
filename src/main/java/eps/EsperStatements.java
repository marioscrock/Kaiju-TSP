package eps;

import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPStatement;

import eps.listener.CEPListenerHL;
import eps.listener.CEPTailSamplingListener;

public class EsperStatements {
	
	/**
	 * Define a named window storing traceId of incoming spans {@code (traceIdHex string)}.
	 * @param cepAdm {@link com.espertech.esper.client.EPAdministrator EPAdministrator} of the Esper engine.
	 * @param retentionTime Sliding time of the window.
	 */
	public static void defineTracesWindow(EPAdministrator cepAdm, String retentionTime) {
		cepAdm.createEPL("create window TracesWindow#unique(traceIdHex)#time(" + retentionTime + ") (traceIdHex string)");
	    cepAdm.createEPL("insert into TracesWindow(traceIdHex)"
	    		+ " select eps.EventToJsonConverter.traceIdToHex(traceIdHigh, traceIdLow)"
	    		+ " from Span");		
	}

	/**
	 * Define a table storing processes of incoming batches {@code (hashProcess int primary key, process thriftgen.Process, hostname string)}.
	 * @param cepAdm {@link com.espertech.esper.client.EPAdministrator EPAdministrator} of the Esper engine.
	 */
	public static void defineProcessesTable(EPAdministrator cepAdm) {
		cepAdm.createEPL("create table ProcessesTable (hashProcess int primary key, process thriftgen.Process, hostname string)");
	    cepAdm.createEPL("on Batch b merge ProcessesTable p"
	    		+ " where eps.EventToJsonConverter.hashProcess(b.process) = p.hashProcess"
	    		+ " when not matched then insert select eps.EventToJsonConverter.hashProcess(process) as hashProcess, process,"
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
		    		+ " select s as span, eps.EventToJsonConverter.hashProcess(p) as hashProcess, p.serviceName as serviceName"
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
	    		+ " select eps.EventToJsonConverter.traceIdToHex(s.span.traceIdHigh, s.span.traceIdLow) as traceIdHexTo,"
	    		+ " s.span.spanId as spanIdTo,"
	    		+ " eps.EventToJsonConverter.traceIdToHex(s.r.traceIdHigh, s.r.traceIdLow) as traceIdHexFrom,"
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
	 * Register a {@link eps.listener.CEPTailSamplingListener CEPTailSamplingListener} saving
	 * spans exiting from {@code SpansWindow} to file.
	 * @param cepAdm {@link com.espertech.esper.client.EPAdministrator EPAdministrator} of the Esper engine.
	 * @param filepath Filepath of the file to save spans.
	 */
	public static void tailSampling(EPAdministrator cepAdm, String filepath) {
	    EPStatement tailSampling = cepAdm.createEPL("select rstream * from SpansWindow as s"
	    		+ " where exists (select * from TracesToBeSampledWindow "
	    		+ "where traceId = (eps.EventToJsonConverter.traceIdToHex(s.span.traceIdHigh, s.span.traceIdLow)))");
	    tailSampling.addListener(new CEPTailSamplingListener(filepath)); 		
	}
	
	public static void reportHLEvents(EPAdministrator cepAdm) {
		EPStatement hlEvent = cepAdm.createEPL("select * from Event");
		hlEvent.addListener(new CEPListenerHL());
	}

	public static void defaultStatementsTraces(EPAdministrator cepAdm, String retentionTime) {
		
		//Event for traces sampling
		cepAdm.createEPL("create schema TraceAnomaly(traceId string)");
		
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
	    //EsperStatements.defineMeanDurationPerOperationTableResetCounter(cepAdm, 1000);
	    
	    //TRACES TO BE SAMPLED WINDOW (traceId)
	    EsperStatements.defineTracesToBeSampledWindow(cepAdm, retentionTime);
	    
	    // TAIL SAMPLING
	    EsperStatements.tailSampling(cepAdm, "./sampled.txt");   
		
	}

	public static void defaultStatementsMetrics(EPAdministrator cepAdm, String retentionTime) {
		
//		DEBUG STATEMENT
//	    EPStatement cepMetrics = cepAdm.createEPL("select * from Metric"); 
//	    cepMetrics.addListener(new CEPListener("Metric: "));	
	    
	}

	public static void defaultStatementsLogs(EPAdministrator cepAdm, String retentionTime) {
		
//		DEBUG STATEMENT
//		EPStatement cepFLogs = cepAdm.createEPL("select * from FLog"); 
//	    cepFLogs.addListener(new CEPListener("FLog: "));
	 
	}

	public static void defaultStatementsHighLevel(EPAdministrator cepAdm, String retentionTime) {
	    
//	    PATTERN
	    
	}

}
