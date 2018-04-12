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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class JsonTracesWS implements Runnable {

    // this set is shared between sessions and threads, so it needs to be thread-safe (http://stackoverflow.com/a/2688817)
    static ConcurrentHashSet<Session> clientSet;
    static AtomicBoolean queueOn; 
    static Thread pullingQueue;
    private static JSONObject jsonContext;
    private static BlockingQueue<JSONObject> queue;
    
    @Override
	public void run() {
    	
    	clientSet = new ConcurrentHashSet<>();
    	queue = new LinkedBlockingQueue<>();
    	queueOn = new AtomicBoolean(false);
    	pullingQueue = new Thread();
    	
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
    
    
    public static void sendBatch(JSONObject batch) {
    	
    	queue.add(batch);
    	
    	if (!pullingQueue.isAlive() && queueOn.get()) {
    		
    		pullingQueue = new Thread(new Runnable() {
        		
        		@Override
        		public void run() {
        			System.out.println("RUNNING");
        			while (queueOn.get()) {
        				JsonTracesWS.pullQueue();
        			}
        			System.out.println("STOPPING");
        			return;
        		}
        	
            });
    		
    		pullingQueue.start();
    		
    	}
    
    	queueOn.set(clientSet.size() > 0);
    	
    }
    
    @SuppressWarnings("unchecked")
    public static void pullQueue() {
    	
    	JSONObject batch = null;
		try {
			batch = queue.take();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
    	batch.put("@context", jsonContext);
    	
    	JsonLdOptions options = new JsonLdOptions();
        //Call whichever JSONLD function you want! (e.g. compact)
        //Object compact = JsonLdProcessor.compact(batch, jsonContext, options);
    	Object expand = JsonLdProcessor.expand(batch, options);
        
        try {
        	//broadcastMessage(JsonUtils.toString(compact));
			broadcastMessage(JsonUtils.toString(expand));
			//broadcastMessage(batch.toJSONString());
		} catch (IOException e) {
			e.printStackTrace();
		}                    

    }

}