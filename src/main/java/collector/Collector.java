package collector;

import com.uber.tchannel.api.TChannel;

public class Collector {

	public static void main(String[] args) throws InterruptedException {
		
		// create TChannel for server
		TChannel tchannel = new TChannel.Builder("jaeger-collector")
				.setServerPort(2042)
				.build();

		tchannel.makeSubChannel("jaeger-collector")
			.register("Collector::submitBatches", new CollectorHandler());

		// listen for incoming connections
		tchannel.listen();//.channel().closeFuture().sync();
       //tchannel.shutdown(false);
		
	}

}
