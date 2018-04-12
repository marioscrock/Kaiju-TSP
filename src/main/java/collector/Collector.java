package collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uber.tchannel.api.TChannel;

import websocket.JsonTracesWS;

public class Collector {
	
	private final static Logger log = LoggerFactory.getLogger(Collector.class);

	public static void main(String[] args) throws InterruptedException {

//		TEST
//		Batch batch = new Batch();
//		batch.process = new thriftgen.Process();
//		batch.process.serviceName = "MarioService";
//		batch.process.tags = new ArrayList<Tag>();
//		batch.process.tags.add((new Tag("Ciao", TagType.STRING)).setVStr("Tag prova"));
//		
//		batch.spans = new ArrayList<Span>();
//		batch.spans.add(new Span(Long.valueOf(22332), Long.valueOf(324252), Long.valueOf(42342342), 
//				Long.valueOf(32423423), "423422", 23423, Long.valueOf(423423), Long.valueOf(4234232)));
//		
//		List<Batch> batchList = new ArrayList<Batch>();
//		batchList.add(batch);
//		try {
//			(new CollectorHandler()).submitBatches(batchList);
//		} catch (TException e) {
//			e.printStackTrace();
//		}
		
		//Open WebSocket
		JsonTracesWS ws = new JsonTracesWS();
		Thread webSocketThread = new Thread(ws);
    	webSocketThread.run();
		
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
