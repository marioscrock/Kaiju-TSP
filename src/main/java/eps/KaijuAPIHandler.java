package eps;

import com.espertech.esper.client.EPOnDemandQueryResult;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;

import eps.listener.CEPListener;
import spark.Request;
import spark.Response;

public class KaijuAPIHandler {
	
	public static String registerStatement(Request request, Response response) {
		
		EPStatement statement = EsperHandler.cepAdm.createEPL(request.queryParams("statement"));
	    statement.addListener(new CEPListener(request.queryParams("msg")));
		
	    response.type("application/json");
		response.status(200);
		
		return "{ \"statementName\":\"" + statement.getName() + "\"}";
	}
	public static String removeStatement(Request request, Response response) throws Exception {
		
		String stmtName = request.queryParams("statement");
		EPStatement statement = EsperHandler.cepAdm.getStatement(stmtName);
		if (statement != null) {
		    statement.destroy();
		    response.type("application/json");
			response.status(200);
		} else {
			throw new Exception("No statement '" + stmtName + "' found");
		}
		
		return "{}";
	}
	
	public static String getAllTracesIds(Request request, Response response) {
		
		EPOnDemandQueryResult result = null;
		
		result = KaijuAPIQueries.preparedTraces.execute();
	
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
	}
	
	public static String getTraceByTraceId(Request request, Response response) {
		
		response.type("application/json");
        
        String traceId = request.params(":id");
        EPOnDemandQueryResult result = null;
        
        synchronized (response) {
        	KaijuAPIQueries.preparedSpansTraceId.setObject(1, traceId);
        	result = EsperHandler.cepRT.executeQuery(KaijuAPIQueries.preparedSpansTraceId);
        }
        

		if (result != null) 
            if (result.getArray().length > 0) {
            	
            	StringBuilder sb = new StringBuilder();
            	sb.append("{ \"spans\":[");
            	
            	for (EventBean e : result.getArray()) {
            		sb.append(" ");
            		sb.append(EventToJsonConverter.spanFromEB(e));
            		sb.append(",");
            	}
            		
            	sb.deleteCharAt(sb.length() - 1);
            	sb.append("] }");
            	
            	return sb.toString();
            } 
        
		return "{ \"spans\":[]}";
	}

	public static String getTracesByServiceName(Request request, Response response) {
    	
		response.type("application/json");
        
        String serviceName = request.queryParams("service");
        EPOnDemandQueryResult result = null;
        
        synchronized (response) {
        	KaijuAPIQueries.preparedSpansServiceName.setObject(1, serviceName);     	
        	result = EsperHandler.cepRT.executeQuery(KaijuAPIQueries.preparedSpansServiceName);
        }
        

		if (result != null) 
            if (result.getArray().length > 0) {
            	StringBuilder sb = new StringBuilder();
            	sb.append("{ \"spans\":[");
            	
            	for (EventBean e : result.getArray()) {
            		sb.append(" ");
            		sb.append(EventToJsonConverter.spanFromEB(e));
            		sb.append(",");
            	}
            		
            	sb.deleteCharAt(sb.length() - 1);
            	sb.append("] }");
            	
            	return sb.toString();
            } 
        
        return "{ \"spans\":[]}";

	}
	
	public static String getLogsByKey(Request request, Response response) {
        
		String key = request.params(":key");
		EPOnDemandQueryResult result = null;

		KaijuAPIQueries.preparedLogs.setObject(1, key);
        	
    	result = EsperHandler.cepRT.executeQuery(KaijuAPIQueries.preparedLogs);
		
		StringBuilder sb = new StringBuilder();
		for (EventBean e : result.getArray())
			sb.append(e.getUnderlying() + "\n");
        
		response.type("application/json");
        
		return "{ \"result\" : \"" + sb.toString() + "\"}";

	}
	
	public static String getDependenciesByTraceId(Request request, Response response) {
		
		String traceId = request.params(":traceId");
		EPOnDemandQueryResult result = null;

		KaijuAPIQueries.preparedDependencies.setObject(1, traceId);
        	
    	result = EsperHandler.cepRT.executeQuery(KaijuAPIQueries.preparedDependencies);
		
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
	}

}
