package collector;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.TException;

import com.google.gson.Gson;

import thriftgen.Batch;
//import websocket.JsonTracesWS;
import websocket.JsonTracesWS;

/**
 * Main class to launch a fake kaiju-collector instance. It emulates incoming batches 
 * reading them from a JSON file.
 * @author Mario
 */
public class FakeCollector {
	
	private final static String FILEPATH = "/dumpTraces.json";
	private static int sentBatches;
	private static List<Batch> batches;
	private static CollectorHandler ch;
	

	public static void main(String[] args) throws InterruptedException {
		
		sentBatches = 0;
		batches = new ArrayList<>();
		
		//Open WebSocket
		JsonTracesWS ws = new JsonTracesWS();
		Thread webSocketThread = new Thread(ws);
    	webSocketThread.start();
    	
    	ch = new CollectorHandler();
    	ch.setWebSocket(true);
    	
		//Read batches from file
		Gson gson = new Gson();
		InputStream in = FakeCollector.class.getResourceAsStream(FILEPATH);
		Batch[] batchesArray = gson.fromJson(new BufferedReader(new InputStreamReader(in)), Batch[].class);
		batches = Arrays.asList(batchesArray);
		
		//Schedule batches
		final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
	    executorService.scheduleAtFixedRate(FakeCollector::sendBatch, 0, 500, TimeUnit.MILLISECONDS);
	  
		return;

	}
	
	/**
	 * Send a batch to the handler. Unit of work for the executor.
	 */
	private static void sendBatch() {
	    
		if (sentBatches < batches.size()) {
			List<Batch> batchesToSend = new ArrayList<Batch>();
			batchesToSend.add(batches.get(sentBatches));
			try {
				ch.submitBatches(batchesToSend);
			} catch (TException e) {
				e.printStackTrace();
			}	
			sentBatches += 1;
		}
	}



}
