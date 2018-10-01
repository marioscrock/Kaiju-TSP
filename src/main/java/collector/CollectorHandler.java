package collector;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.TException;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uber.tchannel.api.handlers.ThriftRequestHandler;
import com.uber.tchannel.messages.ThriftRequest;
import com.uber.tchannel.messages.ThriftResponse;

import eps.EsperHandler;
import thriftgen.Batch;
import thriftgen.BatchSubmitResponse;
import thriftgen.Collector;
import websocket.JsonTracesWS;

public class CollectorHandler extends ThriftRequestHandler<Collector.submitBatches_args, Collector.submitBatches_result> implements Collector.Iface{
	
	private final static Logger log = LoggerFactory.getLogger(CollectorHandler.class);
	
	private RecordCollector thriftTimingCollector;
	private RecordCollector jsonTimingCollector;
	private AtomicInteger numbBatches;
		
	public CollectorHandler() {
		thriftTimingCollector = new RecordCollector("./thriftTiming.csv", 200);
		jsonTimingCollector = new RecordCollector("./jsonTiming.csv", 200);
		numbBatches = new AtomicInteger(0);
		EsperHandler.initializeHandler();
	}
	
	@Override
	public List<BatchSubmitResponse> submitBatches(List<Batch> batches) throws TException {
		
		for(Batch batch : batches) {
			
			//ESPER
			//log.info("Batch to esper");
			EsperHandler.sendBatch(batch);

			//log.info("Batch to JsonLD to WebSocket");
			try {
				JsonTracesWS.sendBatch(batchToJson(batch)); 
			} catch (Exception e) {
				log.error(e.getMessage(), e);
			}
			
//			//SERIALIZE BATCH to JSON
//			BatchSerialize.numBatchToSerialize = 180;
//			BatchSerialize.serialize(batch, numbBatches);
					
		}
			
		return null;
		
	}
	
	private JSONObject batchToJson(Batch batch) throws Exception {
		
		String[] timing = new String[3];
		timing[1] = Long.toString(Instant.now().toEpochMilli());
		JSONObject b = JsonDeserialize.batchToJson(batch);
		timing[2] = Long.toString(Instant.now().toEpochMilli());
		timing[0] = Integer.toString(numbBatches.get());

		jsonTimingCollector.addRecord(timing);
		
		return b;
	}
	
	private List<Batch> deserialize(ThriftRequest<Collector.submitBatches_args> request) {
		
		String[] timing = new String[4];
		timing[1] = Long.toString(Instant.now().toEpochMilli());
		List<Batch> batches = request.getBody(Collector.submitBatches_args.class).getBatches();
		timing[2] = Long.toString(Instant.now().toEpochMilli());
		numbBatches.getAndAdd(batches.size());
		timing[0] = Integer.toString(numbBatches.get());
		
		int batchSpansNum = 0;
		if (batches != null && batches.size() > 0) {
			for (Batch b : batches)
				batchSpansNum += b.getSpansSize();
		}
		timing[3] = Integer.toString(batchSpansNum);
		thriftTimingCollector.addRecord(timing);
		
		return batches;
	}
	

	@Override
	public ThriftResponse<Collector.submitBatches_result> handleImpl(ThriftRequest<Collector.submitBatches_args> request) {
		
		List<Batch> batches = deserialize(request);
		
		collector.Collector.executor.execute(new Runnable() {
				
			@Override
			public void run() {
				try {
					submitBatches(batches);
				} catch (TException e) {
					e.printStackTrace();
				}		
			}
			
		});
		
		return new ThriftResponse.Builder<Collector.submitBatches_result>(request)
	            .setBody(new Collector.submitBatches_result())
	            .build();
		
	}

}
