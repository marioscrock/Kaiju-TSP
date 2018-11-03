package websocket;

import org.eclipse.jetty.websocket.api.Session;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;

import spark.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Class managing a web socket at url {@code :4567/streams/jsonTraces} outputting incoming data in JSON-LD format
 * expandable through the {@code tracing_ontology_context.json} context. If {@link #isContextSet()} is {@code true} 
 * the context is sent together with data, otherwise data are sent without the context. To set the context a valid 
 * path for a JSON file containing the context should be provided through {@link #setContext(String)} before calling the 
 * {@link #run()} method.
 * @author Mario
 *
 */
public class JsonTracesWS implements Runnable {
	
	private final static Logger log = LoggerFactory.getLogger(JsonTracesWS.class);
	
    // This set is shared between sessions and threads, so it needs to be thread-safe (http://stackoverflow.com/a/2688817)
    protected static Set<Session> clientSet;
    protected static AtomicBoolean queueOn; 
    protected static Thread pullingQueue;
    
    private static boolean context = false;
    private static String contextPath;
    private static JSONObject jsonContext;
    private static BlockingQueue<JSONObject> queue;
    
    /**
     * Runnable implementation initializing the web socket.
     */
	@Override
	public void run() {
    	
    	clientSet = ConcurrentHashMap.newKeySet();
    	queue = new LinkedBlockingQueue<>();
    	queueOn = new AtomicBoolean(false);
    	pullingQueue = new Thread();
    	
    	if (context) {
	    	JSONParser parser = new JSONParser();
	
	    	Object obj = null;
			try {
				InputStream in = getClass().getResourceAsStream(contextPath);
				obj = parser.parse(new BufferedReader(new InputStreamReader(in)));
	        } catch (IOException | ParseException e) {
	        	log.error("Error parsing context: " + e.getMessage());
	        }
			
	    	JSONObject jsonObject = (JSONObject) obj;
	    	jsonContext = (JSONObject) jsonObject.get("@context");
	    }
        
    	Service http = Service.ignite();
        http.webSocket("/streams/jsonTraces", WebSocketHandler.class);
        http.init();
        
	}
    
    /**
     * Static method to send a JSON representation of {@code thriftgen.Batch} objects to connected clients.
     * @param batch
     */
    public static void sendBatch(JSONObject batch) {
    	
    	queue.add(batch);
    	
    	if (!pullingQueue.isAlive() && queueOn.get()) {
    		
    		pullingQueue = new Thread(new Runnable() {
        		
        		@Override
        		public void run() {
        			while (queueOn.get()) {
        				try {
							JsonTracesWS.pullQueue();
						} catch (IOException e) {
							e.printStackTrace();
						}
        			}
        			return;
        		}
        	
            });
    		
    		pullingQueue.start();
    		
    	}
    
    	queueOn.set(clientSet.size() > 0);
    	
    }
    
    /**
	 * Static method to broadcast messages to connected clients.
	 * @param message
	 */
    protected static void broadcastMessage(String message) {
        clientSet.stream().filter(Session::isOpen).forEach(session -> {
            try {
                session.getRemote().sendString(message);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
    
    /**
     * Static method to pull the queue of {@link org.json.simple.JSONObject JSONObject}s to be sent.
     * @throws JsonGenerationException
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
	private static void pullQueue() throws JsonGenerationException, IOException {
    	
    	JSONObject batch = null;
		try {
			batch = queue.take();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		String msg = JsonUtils.toString(batch);
		
		if (context) {
			
	    	batch.put("@context", jsonContext);
	    	JsonLdOptions options = new JsonLdOptions();
	    	
	        //Call whichever JSONLD function you want! (e.g. compact)
	        //Object compact = JsonLdProcessor.compact(batch, jsonContext, options);
	    	Object expand = JsonLdProcessor.expand(batch, options);
	    	msg = JsonUtils.toString(expand);
		}
        
		broadcastMessage(msg);
                 
    }
    
    /**
     * Static method to check if a context is set and it is sent together with JSON data.
     * @return {@code true} if a context is set and is sent together with JSON data. 
     */
    public static boolean isContextSet() {
		return context;
	}
    
    /**
     * Static method to set a context to be sent together with JSON data.
     * @param contextPath path of JSON file containing the context.
     */
	public static void setContext(String contextPath) {
		if (contextPath != null) {
			JsonTracesWS.context = true;
			JsonTracesWS.contextPath = contextPath;
		}
	}

}