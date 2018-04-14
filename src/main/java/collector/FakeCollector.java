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
import websocket.JsonTracesWS;

public class FakeCollector {
	
	private static int sentBatches = 0;
	private static List<Batch> batches = new ArrayList<>();
	private static CollectorHandler ch = new CollectorHandler();

	public static void main(String[] args) {
		
		//Open WebSocket
		JsonTracesWS ws = new JsonTracesWS();
		Thread webSocketThread = new Thread(ws);
    	webSocketThread.start();
		
		Gson gson = new Gson();
		

		InputStream in = FakeCollector.class.getResourceAsStream("/dumpTraces.json");
		Batch[] batchesArray = gson.fromJson(new BufferedReader(new InputStreamReader(in)), Batch[].class);
		batches = Arrays.asList(batchesArray);
		
		final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
	    executorService.scheduleAtFixedRate(FakeCollector::sendBatch, 0, 1000, TimeUnit.MICROSECONDS);
	  
		return;

	}

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
