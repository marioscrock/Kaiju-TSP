package collector;

import java.util.HashSet;
import java.util.List;

import org.apache.thrift.TException;

import com.uber.tchannel.api.handlers.ThriftRequestHandler;
import com.uber.tchannel.messages.ThriftRequest;
import com.uber.tchannel.messages.ThriftResponse;

import thriftgen.Batch;
import thriftgen.BatchSubmitResponse;
import thriftgen.Collector;

public class CollectorHandler extends ThriftRequestHandler<Collector.submitBatches_args, Collector.submitBatches_result> implements Collector.Iface{
	
	public CollectorHandler() {};
	public HashSet<Long> traceIds = new HashSet<Long>();

	@Override
	public List<BatchSubmitResponse> submitBatches(List<Batch> batches) throws TException {
		
		System.out.println(batches.size());
		
		for(int i=0; i < batches.size(); i++) {
			System.out.println(batches.get(i).getSpans().size());
			for(int j=0; j < batches.get(i).getSpans().size(); j++) {
				traceIds.add(batches.get(i).getSpans().get(j).traceIdLow);
				System.out.println(batches.get(i).getSpans().get(j).traceIdLow);
				System.out.println("SIZE SET of TRACES: " + traceIds.size()); 
			}
		}
		
		return null;
		
	}

	@Override
	public ThriftResponse<Collector.submitBatches_result> handleImpl(ThriftRequest<Collector.submitBatches_args> request) {
		
		System.out.println("HERE");
		List<Batch> batches = request.getBody(Collector.submitBatches_args.class).getBatches();
		
		try {
			submitBatches(batches);
		} catch (TException e) {
			e.printStackTrace();
		}
		
		return new ThriftResponse.Builder<Collector.submitBatches_result>(request)
	            .setBody(new Collector.submitBatches_result())
	            .build();
		
	}

}
