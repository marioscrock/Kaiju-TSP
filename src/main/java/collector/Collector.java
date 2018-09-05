package collector;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uber.tchannel.api.SubChannel;
import com.uber.tchannel.api.TChannel;


public class Collector {
	
	private final static Logger log = LoggerFactory.getLogger(Collector.class);
	public static ThreadPoolExecutor executor;

	public static void main(String[] args) throws InterruptedException {
		
		//Open WebSocket
		//JsonTracesWS ws = new JsonTracesWS();
		//Thread webSocketThread = new Thread(ws);
    	//webSocketThread.start();
    	
    	//Executors
    	BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>();
    	executor = new ThreadPoolExecutor(3, 3,
    			10000, TimeUnit.MILLISECONDS, workQueue);
    	log.info("Executors pool initialised");
		
		// create TChannel for server
		TChannel tchannel = new TChannel.Builder("kaiju-collector")
				.setServerPort(2042)
				.build();

		SubChannel subCh = tchannel.makeSubChannel("kaiju-collector");
		subCh.register("Collector::submitBatches", new CollectorHandler());
		
		log.info("Handler registered for Collector::submitBatches");
		
		// listen for incoming connections
		tchannel.listen().channel().closeFuture().sync(); //tchannel.listen()
        tchannel.shutdown();
		
	}

}
