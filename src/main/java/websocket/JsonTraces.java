package websocket;

import org.eclipse.jetty.util.ConcurrentHashSet;
import org.eclipse.jetty.websocket.api.Session;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;

import static spark.Spark.*;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class JsonTraces implements Runnable {

    // this set is shared between sessions and threads, so it needs to be thread-safe (http://stackoverflow.com/a/2688817)
    static ConcurrentHashSet<Session> clientSet = new ConcurrentHashSet<>();
    private static JSONObject jsonContext;
    
    @Override
	public void run() {
//    	staticFiles.location("/public"); //index.html is served at localhost:4567 (default port)
//      staticFiles.expireTime(600);
    	
    	JSONParser parser = new JSONParser();

    	Object obj = null;
		try {
			InputStream in = getClass().getResourceAsStream("/tracing_ontology_context.json");
			obj = parser.parse(new BufferedReader(new InputStreamReader(in)));
		} catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
		
    	JSONObject jsonObject = (JSONObject) obj;
    	jsonContext = (JSONObject) jsonObject.get("@context");
            
        webSocket("/jsonTraces", WebSocketHandler.class);
        init();
        
	}
    
    public static void broadcastMessage(String message) {
        clientSet.stream().filter(Session::isOpen).forEach(session -> {
            try {
                session.getRemote().sendString(message);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
    
    @SuppressWarnings("unchecked")
    public static void sendBatch(JSONObject batch) {
    	
        clientSet.stream().filter(Session::isOpen).forEach(session -> {
            try {
            	
            	batch.put("@context", jsonContext);
            	
            	JsonLdOptions options = new JsonLdOptions();
                // Call whichever JSONLD function you want! (e.g. compact)
                //Object compact = JsonLdProcessor.compact(batch, jsonContext, options);
            	Object expand = JsonLdProcessor.expand(batch, options);
                
                //session.getRemote().sendString(JsonUtils.toString(compact));
                session.getRemote().sendString(JsonUtils.toString(expand));
            	
                //session.getRemote().sendString(batch.toJSONString());
                
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }


	

}