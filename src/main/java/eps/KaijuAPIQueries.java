package eps;

import com.espertech.esper.client.EPOnDemandPreparedQuery;
import com.espertech.esper.client.EPOnDemandPreparedQueryParameterized;

public class KaijuAPIQueries {
	
	protected static EPOnDemandPreparedQuery preparedTraces;
	protected static EPOnDemandPreparedQueryParameterized preparedSpansServiceName;
	protected static EPOnDemandPreparedQueryParameterized preparedSpansTraceId;
	
	protected static EPOnDemandPreparedQueryParameterized preparedLogs;
	protected static EPOnDemandPreparedQueryParameterized preparedDependencies;
	
	public static void initPreparedQueries() {
		
		String queryTraces = "select * from TracesWindow"; 
        preparedTraces = EsperHandler.cepRT.prepareQuery(queryTraces);
		
		String querySpansServiceName = "select * from SpansWindow "
				+ "where serviceName = ? order by span.traceIdHigh, span.traceIdLow"; 
        preparedSpansServiceName = EsperHandler.cepRT.prepareQueryWithParameters(querySpansServiceName);
        
        String querySpansTraceId = "select * from SpansWindow "
				+ "where collector.JsonDeserialize.traceIdToHex(span.traceIdHigh, span.traceIdLow) = ?"; 
        preparedSpansTraceId = EsperHandler.cepRT.prepareQueryWithParameters(querySpansTraceId);
        
        String queryLogs = "select distinct span.operationName from " +
        	    " SpansWindow where span.getLogs().anyOf(l => l.getFields().anyOf(f => f.key = ?))"; 
        preparedLogs = EsperHandler.cepRT.prepareQueryWithParameters(queryLogs);
        
        String queryDependencies = "select sFrom.serviceName as serviceFrom, sTo.serviceName as serviceTo, count(*) as numInteractions from"
        	    + " DependenciesWindow(traceIdHexFrom = ?) as d, SpansWindow as sFrom,"
        	    + " SpansWindow as sTo"
        	    + " where sTo.span.spanId = d.spanIdTo and sFrom.span.spanId = d.spanIdFrom"
        	    + " group by sFrom.serviceName, sTo.serviceName"; 
        preparedDependencies = EsperHandler.cepRT.prepareQueryWithParameters(queryDependencies);
        
	}

}
