package eps;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.espertech.esper.client.EPOnDemandPreparedQuery;
import com.espertech.esper.client.EPOnDemandPreparedQueryParameterized;
import com.espertech.esper.client.EPOnDemandQueryResult;
import com.espertech.esper.client.EventBean;

import collector.JsonDeserialize;
import spark.Service;
import thriftgen.Log;
import thriftgen.Span;
import thriftgen.SpanRef;
import thriftgen.Tag;

public class KaijuAPI {
	
	private final static Logger log = LoggerFactory.getLogger(KaijuAPI.class);
	
	private static EPOnDemandPreparedQuery preparedTraces;
	private static EPOnDemandPreparedQueryParameterized preparedSpansServiceName;
	private static EPOnDemandPreparedQueryParameterized preparedSpansTraceId;
	
	private static EPOnDemandPreparedQueryParameterized preparedLogs;
	private static EPOnDemandPreparedQueryParameterized preparedDependencies;
	
	public static void initPreparedQueries() {
		
		String queryTraces = "select * from TracesWindow"; 
        preparedTraces = EsperHandler.cepRT.prepareQuery(queryTraces);
		
		String querySpansServiceName = "select * from SpansWindow "
				+ "where serviceName = ?"; 
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

    public static void initAPI() {
    	
    	initPreparedQueries();
    	
    	Service http = Service.ignite()
    			.port(9278);

    	http.get("/api/traces/all", (request, response) -> {
            
    		EPOnDemandQueryResult result = null;
    		
    		try {
    			result = preparedTraces.execute();
    		} catch (Exception e) {
    			log.info(e.getStackTrace().toString());
    			log.info(e.getMessage());
    		}
            
    		response.type("application/json");
    		if (result != null) 
	            if (result.getArray().length > 0) {
	            	
	            	StringBuilder sb = new StringBuilder();
	            	sb.append("{ \"traceIDsHex\":[");
	            	for (EventBean row : result.getArray()) {
	            		  sb.append(" \"" + row.get("traceIdHex") + "\",");
	            	}
	            	sb.deleteCharAt(sb.length() - 1);
	            	sb.append("] }");
	            	
	            	return sb.toString();
	            	
	            } 
	        
    		return "{ \"traceIDsHex\":[]}";
            
        });

        http.get("/api/traces", (request, response) -> {
        	response.type("application/json");
	        
	        String serviceName = request.queryParams("service");
	        EPOnDemandQueryResult result = null;
	        
	        synchronized (response) {
	        	preparedSpansServiceName.setObject(1, serviceName);
	        	
	        	try {
	        		result = EsperHandler.cepRT.executeQuery(preparedSpansServiceName);
	        	} catch (Exception e) {
	    			log.info(e.getStackTrace().toString());
	    			log.info(e.getMessage());
	    		}
	        }
	
	    	response.type("application/json");
	        
	        try {
	    		if (result != null) 
		            if (result.getArray().length > 0) {
		            	StringBuilder sb = new StringBuilder();
		            	sb.append("{ \"spans\":[");
		            	
		            	for (EventBean e : result.getArray()) {
		            		sb.append(" ");
		            		sb.append(spanFromEB(e));
		            		sb.append(",");
		            	}
		            		
		            	sb.deleteCharAt(sb.length() - 1);
		            	sb.append("] }");
		            	
		            	return sb.toString();
		            } 
	         } catch (Exception e) {
	        	 log.info(e.getStackTrace().toString());
	        	 log.info(e.getMessage());
			 }
	        
	        return "{ \"spans\":[]}";
		    
		});
        
        http.get("/api/traces/:id", (request, response) -> {
            response.type("application/json");
            
            String traceId = request.params(":id");
            EPOnDemandQueryResult result = null;
            
            synchronized (response) {
            	preparedSpansTraceId.setObject(1, traceId);
            	
            	try {
            		result = EsperHandler.cepRT.executeQuery(preparedSpansTraceId);
            	} catch (Exception e) {
        			log.info(e.getStackTrace().toString());
        			log.info(e.getMessage());
        		}
            }
            
            response.type("application/json");
            
            try {
	    		if (result != null) 
		            if (result.getArray().length > 0) {
		            	response.type("application/json");
		            	
		            	StringBuilder sb = new StringBuilder();
		            	sb.append("{ \"spans\":[");
		            	
		            	for (EventBean e : result.getArray()) {
		            		sb.append(" ");
		            		sb.append(spanFromEB(e));
		            		sb.append(",");
		            	}
		            		
		            	sb.deleteCharAt(sb.length() - 1);
		            	sb.append("] }");
		            	
		            	return sb.toString();
		            } 
            } catch (Exception e) {
    			log.info(e.getStackTrace().toString());
    			log.info(e.getMessage());
    		}
	        
    		return "{ \"spans\":[]}";
            
        });
        
        http.get("/api/logs/:key", (request, response) -> {
            
    		String key = request.params(":key");
    		EPOnDemandQueryResult result = null;

			preparedLogs.setObject(1, key);
	        	
        	try {
        		result = EsperHandler.cepRT.executeQuery(preparedLogs);
        	} catch (Exception e) {
    			log.info(e.getStackTrace().toString());
    			log.info(e.getMessage());
    		}
    		
    		StringBuilder sb = new StringBuilder();
    		for (EventBean e : result.getArray())
    			sb.append(e.getUnderlying() + "\n");
            
    		response.type("application/json");
	        
    		return "{ \"result\" : \"" + sb.toString() + "\"}";
            
        });
        
        http.get("/api/dependencies/:traceId", (request, response) -> {
            
    		String traceId = request.params(":traceId");
    		EPOnDemandQueryResult result = null;

			preparedDependencies.setObject(1, traceId);
	        	
        	try {
        		result = EsperHandler.cepRT.executeQuery(preparedDependencies);
        	} catch (Exception e) {
    			log.info(e.getStackTrace().toString());
    			log.info(e.getMessage());
    		}
    		
        	response.type("application/json");
    		if (result != null) 
	            if (result.getArray().length > 0) {
	            	
	            	StringBuilder sb = new StringBuilder();
	            	sb.append("{ \"dependencies\":[");
	            	for (EventBean row : result.getArray()) {
	            		sb.append(" { ");
	    	    		sb.append("\"serviceFrom\" : ");
	    	    		sb.append(" \"" + row.get("serviceFrom") + "\", ");
	    	    		sb.append("\"serviceTo\" : ");
	    	    		sb.append(" \"" + row.get("serviceTo") + "\", ");
	    	    		sb.append("\"numInteractions\" : ");
	    	    		sb.append(" \"" + row.get("numInteractions") + "\"");
	    	    		sb.append("},");
	            	}
	            	sb.deleteCharAt(sb.length() - 1);
	            	sb.append("] }");
	            	
	            	return sb.toString();
	            	
	            } 
	        
    		return "{ \"traceIDsHex\":[]}";
            
        });

    }
    
	public static String spanFromEB(EventBean e) {
    	
    	Span s = (Span) e.get("span");
    	
    	StringBuilder sb = new StringBuilder();
    	sb.append("{ ");
    	sb.append("\"traceID\" : ");
    	sb.append("\"" + JsonDeserialize.traceIdToHex(s.getTraceIdHigh(), s.getTraceIdLow()) + "\", ");
    	sb.append("\"serviceName\" : ");
    	sb.append("\"" + e.get("serviceName") + "\", ");
    	sb.append("\"hashProcess\" : ");
    	sb.append("\"" + e.get("hashProcess") + "\", ");
    	sb.append("\"spanID\" : ");
    	sb.append("\"" + Long.toHexString(s.getSpanId()) + "\", ");
    	sb.append("\"parentSpanID\" : ");
    	sb.append("\"" + Long.toHexString(s.getParentSpanId()) + "\", ");
    	sb.append("\"operationName\" : ");
    	sb.append("\"" + s.getOperationName() + "\", ");
    	
    	sb.append("\"references\" : [");
    	List<SpanRef> spanrefs =  s.getReferences();
    	if (spanrefs.size() > 0) {
	    	for(int i = 0; i < spanrefs.size(); i ++) {
	    		sb.append(" { ");
	    		sb.append("\"traceID\" : ");
	    		sb.append(" \"" + JsonDeserialize.traceIdToHex(spanrefs.get(i).getTraceIdHigh(), spanrefs.get(i).getTraceIdLow()) + "\", ");
	    		sb.append("\"spanID\" : ");
	    		sb.append(" \"" + Long.toHexString(spanrefs.get(i).getSpanId()) + "\", ");
	    		sb.append("\"refType\" : ");
	    		sb.append(" \"" + spanrefs.get(i).getRefType().toString() + "\"");
	    		sb.append("},");
	    	}
	    	sb.deleteCharAt(sb.length() - 1);   	
    	} 
    	sb.append(" ], ");
    	
    	sb.append("\"startTime\" : ");
    	sb.append("\"" + s.getStartTime() + "\", ");
    	sb.append("\"duration\" : ");
    	sb.append("\"" + s.getDuration() + "\", ");
    	sb.append("\"flags\" : ");
    	sb.append("\"" + s.getFlags() + "\", ");	
    	
    	sb.append("\"tags\" : [");
    	List<Tag> tags =  s.getTags();
    	if (tags.size() > 0) {
	    	for(int i = 0; i < tags.size(); i ++) {
	    		sb.append(" { ");
	    		sb.append("\"key\" : ");
	    		sb.append(" \"" + tags.get(i).getKey() + "\", ");
	    		sb.append("\"value\" : ");
	    		sb.append(" \"" + tags.get(i).getKey() + "\", ");
	    		sb.append("\"type\" : ");
	    		sb.append(" \"" + tags.get(i).getVType().toString() + "\"");
	    		sb.append("},");
	    	}
	    	sb.deleteCharAt(sb.length() - 1);
    	} 
    	sb.append(" ], ");
    	
    	sb.append("\"logs\" : [");
    	List<Log> logs =  s.getLogs();
    	if (logs.size() > 0) {
	    	for(int i = 0; i < logs.size(); i ++) {
	    		sb.append("{ ");
	    		sb.append("\"timestamp\" : ");
	    		sb.append(" \"" + logs.get(i).getTimestamp() + "\", ");
	    		sb.append("\"fields\" : [");
	    		for (Tag f : logs.get(i).getFields()) {
	    			sb.append(" { ");
		    		sb.append("\"key\" : ");
		    		sb.append(" \"" + f.getKey() + "\", ");
		    		sb.append("\"value\" : ");
		    		sb.append(" \"" + f.getKey() + "\", ");
		    		sb.append("\"type\" : ");
		    		sb.append(" \"" + f.getVType().toString() + "\"");
		    		sb.append("},");
	    		}
	    		sb.deleteCharAt(sb.length() - 1);
	        	sb.append(" ]");	
	    		
	        	sb.append("},");
	    	}
	    	sb.deleteCharAt(sb.length() - 1);
    	} 
    	sb.append(" ]");
    	
    	sb.append(" }");   	
    	return sb.toString();
    	
    }

}
