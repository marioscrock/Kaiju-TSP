package eps;

import spark.Service;

public class KaijuAPI {

    public static void initAPI() {
    	
    	KaijuAPIQueries.initPreparedQueries();
    	
    	Service http = Service.ignite()
    			.port(9278);
    	
    	// EXCEPTIONS
		http.exception(Exception.class, (e, request, response) -> {
			response.type("application/json");
			response.status(400);
			response.body("{ \"exception\":\"" + e.getClass() + "\","
					+ "\"message\":\"" + e.getMessage() + "\""
					+ "}");
		});
    	
		// POST /api/statement
    	http.post("/api/statement", (request, response) -> KaijuAPIHandler.registerStatement(request, response));
    	
    	// POST /api/remove
    	http.post("/api/remove", (request, response) -> KaijuAPIHandler.removeStatement(request, response));
    	
    	// GET /api/traces/all
    	http.get("/api/traces/all", (request, response) -> KaijuAPIHandler.getAllTracesIds(request, response));
    	
    	// GET /api/traces?service=<service>&limit=<limit> (default limit equal to 10)
        http.get("/api/traces", (request, response) -> KaijuAPIHandler.getTracesByServiceName(request, response));
        
        // GET /api/traces/:id
        http.get("/api/traces/:id", (request, response) -> KaijuAPIHandler.getTraceByTraceId(request, response));
        
        // GET /api/logs/:key
        http.get("/api/logs/:key", (request, response) -> KaijuAPIHandler.getLogsByKey(request, response));
        
        // GET /api/dependencies/:traceId
        http.get("/api/dependencies/:traceId", (request, response) -> KaijuAPIHandler.getDependenciesByTraceId(request, response));

    }

}
