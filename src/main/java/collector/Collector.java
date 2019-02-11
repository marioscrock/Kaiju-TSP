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

/**
 * Main class to launch the kaiju-collector instance.
 * args[0] sets the mode for Kaiju: traces (or traces-api), logs, metrics or high-level (default is "traces").
 * args[1] sets the retention time for Esper windows (default is 2 min).
 * @author Mario
 */
public class Collector {
	
	private final static Logger log = LoggerFactory.getLogger(Collector.class);
	
	public static ThreadPoolExecutor executor;	
	private static CollectorHandler ch;

	public static void main(String[] args) throws InterruptedException {
		
		//Set mode Esper
		if(args.length > 0) {
			EsperHandler.mode = args[0];
			log.info("Esper mode set: " + args[0]);
		}
		
		//Set retention time Esper
		if(args.length > 1) {
			EsperHandler.retentionTime = args[1];
			log.info("Esper retention time set: " + args[1]);
		}

    	//Executors to handle incoming requests
    	BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>();
    	executor = new ThreadPoolExecutor(5, 5,
    			10000, TimeUnit.MILLISECONDS, workQueue);
    	log.info("Executors pool initialised");
		
		ch = new CollectorHandler();	
		
		if (EsperHandler.mode.equals("traces")) {
			//Create TChannel to serve jaeger-agents
			TChannel tchannel = new TChannel.Builder("kaiju-collector")
					.setServerPort(2042)
					.build();
			
			ch.setThriftTiming(true);
			
			//Register Handler for submitBatch interface defined in thrift file
			SubChannel subCh = tchannel.makeSubChannel("kaiju-collector");
			subCh.register("Collector::submitBatches", ch);
			log.info("Handler registered for Collector::submitBatches");
			
			// listen for incoming connections
			tchannel.listen().channel().closeFuture().sync(); //tchannel.listen()
	        tchannel.shutdown();
	        
    	} else {
    		
    		if (EsperHandler.mode.equals("metrics"))
    			//Port for metrics
    			EventSocket.port = 9876;
    		else
    			//Port for logs
    			EventSocket.port = 24224;
    			
			//Open Events Socket
	    	EventSocket es = new EventSocket();
	    	Thread eventSocketThread = new Thread(es);
	    	eventSocketThread.start();
	    	log.info("Socket thread started");
    	}
		
	}

}
