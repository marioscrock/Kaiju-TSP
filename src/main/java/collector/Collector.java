package collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uber.tchannel.api.TChannel;

import websocket.JsonTracesWS;

public class Collector {
	
	private final static Logger log = LoggerFactory.getLogger(Collector.class);

	public static void main(String[] args) throws InterruptedException {
		
		//Open WebSocket
		JsonTracesWS ws = new JsonTracesWS();
		Thread webSocketThread = new Thread(ws);
    	webSocketThread.start();
		
		// create TChannel for server
		TChannel tchannel = new TChannel.Builder("jaeger-collector")
				.setServerPort(2042)
				.build();

		tchannel.makeSubChannel("jaeger-collector")
			.register("Collector::submitBatches", new CollectorHandler());
		
		log.info("Handler registered for Collector::submitBatches");
		// listen for incoming connections
		tchannel.listen();//.channel().closeFuture().sync();
       //tchannel.shutdown(false);
		
	}

}
