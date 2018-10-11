package collector;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uber.tchannel.api.SubChannel;
import com.uber.tchannel.api.TChannel;

import eps.EsperHandler;
import eventsocket.EventSocket;
import websocket.JsonTracesWS;


public class Collector {
	
	private final static Logger log = LoggerFactory.getLogger(Collector.class);
	public static ThreadPoolExecutor executor;

	public static void main(String[] args) throws InterruptedException {
		
		//Open WebSocket
//		JsonTracesWS ws = new JsonTracesWS();
//		Thread webSocketThread = new Thread(ws);
//    	webSocketThread.start();
		
		//Set retention time Esper
		if(args.length == 1) {
			EsperHandler.retentionTime = args[0];
			log.info("Esper retention time set: " + args[0]);
		}

    	//Executors
    	BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>();
    	executor = new ThreadPoolExecutor(5, 5,
    			10000, TimeUnit.MILLISECONDS, workQueue);
    	log.info("Executors pool initialised");
		
		// create TChannel for server
		TChannel tchannel = new TChannel.Builder("kaiju-collector")
				.setServerPort(2042)
				.build();

		SubChannel subCh = tchannel.makeSubChannel("kaiju-collector");
		subCh.register("Collector::submitBatches", new CollectorHandler());
		log.info("Handler registered for Collector::submitBatches");
		
		//Open Events and Metrics Socket
    	EventSocket es = new EventSocket();
    	Thread eventSocketThread = new Thread(es);
    	eventSocketThread.start();
		
		// listen for incoming connections
		tchannel.listen().channel().closeFuture().sync(); //tchannel.listen()
        tchannel.shutdown();
		
	}

}
