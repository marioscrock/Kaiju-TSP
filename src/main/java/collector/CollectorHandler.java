package collector;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uber.tchannel.api.handlers.ThriftRequestHandler;
import com.uber.tchannel.messages.ThriftRequest;
import com.uber.tchannel.messages.ThriftResponse;

import eps.EsperHandler;
import thriftgen.Batch;
import thriftgen.BatchSubmitResponse;
import thriftgen.Collector;

public class CollectorHandler extends ThriftRequestHandler<Collector.submitBatches_args, Collector.submitBatches_result> implements Collector.Iface{
	
	public EsperHandler esperHandler;
	
	private final static Logger log = LoggerFactory.getLogger(CollectorHandler.class);
	private AtomicInteger numbBatches = new AtomicInteger(0);
	private TimingCollector thriftTimingCollector = new TimingCollector("/thriftTiming.csv");
	
	public CollectorHandler() {
		esperHandler = new EsperHandler();
		esperHandler.initializeHandler();
	}
	
	@Override
	public List<BatchSubmitResponse> submitBatches(List<Batch> batches) throws TException {
		
		for(Batch batch : batches) {
			
			//ESPER
			log.info("Batch to esper");
			esperHandler.sendBatch(batch);
					
//			try {
//				JSONObject b = JsonDeserialize.batchToJson(batch);
//				JsonTracesWS.sendBatch(b); 
//			} catch (Exception e) {
//				log.error(e.getMessage(), e);
//			}
			
//			//SERIALIZE BATCH to JSON
//			BatchSerialize.numBatchToSerialize = 180;
//			BatchSerialize.serialize(batch, numbBatches);
					
		}
			
		return null;
		
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
