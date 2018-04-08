package collector;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;

import org.apache.thrift.TException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.uber.tchannel.api.handlers.ThriftRequestHandler;
import com.uber.tchannel.messages.ThriftRequest;
import com.uber.tchannel.messages.ThriftResponse;

import thriftgen.Batch;
import thriftgen.BatchSubmitResponse;
import thriftgen.Collector;
import thriftgen.Log;
import thriftgen.Span;
import thriftgen.SpanRef;
import thriftgen.SpanRefType;
import thriftgen.Tag;
import thriftgen.TagType;

public class CollectorHandler extends ThriftRequestHandler<Collector.submitBatches_args, Collector.submitBatches_result> implements Collector.Iface{
	
	public CollectorHandler() {};
	public HashSet<Long> traceIds = new HashSet<Long>();
	public int numbBatches = 0;
	public static final String PREFIX_LOG = "tr:log/";
	public static final String PREFIX_SPAN = "tr:span/";
	public static final String PREFIX_TAG = "tr:tag/";
	public static final String PREFIX_TRACE = "tr:trace/";
	
	@Override
	public List<BatchSubmitResponse> submitBatches(List<Batch> batches) throws TException {
		
		System.out.println("Processing batch number: " + numbBatches + "\nBatch size: " + batches.size());
		
		JSONObject obj = new JSONObject();
		
		for(Batch batch : batches) {
			batchToJson(obj, batch);
		}

        try (FileWriter file = new FileWriter("test" + numbBatches + ".json")) {

            file.write(obj.toJSONString());
            file.flush();

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.print(obj);
		
		return null;
		
	}

	private void batchToJson(JSONObject obj, Batch batch) {
		
		thriftgen.Process process = batch.getProcess();
		processToJson(obj, process); //Add JSONLD representation of process to obj
		
		//TODO Add all traces to obj
		//Need to keep a list of already seen traces?
		
		List<Span> spans = batch.getSpans();
		//TODO Change null with process @id
		spansToJson(obj, spans, null);  //Add JSONLD representation of spans to obj
		
	}
	
	@SuppressWarnings("unchecked")
	private void processToJson(JSONObject obj, thriftgen.Process process) {
		
		//TODO ADD id to the PROCESS! Scan all traces and add all ids traceId+processId? 
		//We DO NOT have process Ids at this point! Review Process definition!
		JSONObject jsonProcess = new JSONObject();
		jsonProcess.put("serviceName", process.getServiceName());
		
		List<Tag> tags = process.getTags();
		JSONArray jsonTags = tagsToJson(tags);
		jsonProcess.put("hasProcessTag", jsonTags);
		
		obj.put("Process", jsonProcess);

	}
	
	@SuppressWarnings("unchecked")
	private JSONArray tagsToJson(List<Tag> tags) {
		
		JSONArray jsonTags = new JSONArray();
		
		for(Tag tag : tags) {
			
			JSONObject jsonTag = new JSONObject();
			String id = tag.getKey();
			jsonTag.put("key", tag.getKey());
			
			if (tag.vType.equals(TagType.STRING)){
				jsonTag.put("stringVal", tag.vStr);
				id += tag.vStr;
			}
			if (tag.vType.equals(TagType.DOUBLE)){
				jsonTag.put("doubleVal", tag.vDouble);
				id += tag.vDouble;
			}
			if (tag.vType.equals(TagType.BOOL)){
				jsonTag.put("boolVal", tag.vBool);
				id += tag.vBool;
			}
			if (tag.vType.equals(TagType.LONG)){
				jsonTag.put("longVal", tag.vLong);
				id += tag.vLong;
			}
			if (tag.vType.equals(TagType.BINARY)){
				jsonTag.put("binaryVal", tag.vBinary);
				id += tag.vBinary;
			}
			
			jsonTag.put("@id", PREFIX_TAG + id);
			jsonTags.add((new JSONObject()).put("Tag", jsonTag));
					
		}
		
		return jsonTags;
		
	}

	@SuppressWarnings("unchecked")
	private void spansToJson(JSONObject obj, List<Span> spans, String processId) {
		
		JSONArray jsonSpans = new JSONArray();
		
		for(Span span : spans) {
			
			JSONObject jsonSpan = new JSONObject();
			
			//ATTENTION: in DB no used high-low but hex representation
			String id = Long.toString(span.getTraceIdHigh()) + Long.toString(span.getTraceIdLow()) + Long.toString(span.getSpanId());
			
			//Data properties
			jsonSpan.put("@id", PREFIX_SPAN + id);
			jsonSpan.put("spanId", span.getSpanId());
			jsonSpan.put("operationName", span.getOperationName());
			jsonSpan.put("startTime", span.getStartTime());
			jsonSpan.put("duration", span.getDuration());
			jsonSpan.put("flags", span.getFlags());
			
			//Object properties
			jsonSpan.put("spanOfProcess", processId);
			jsonSpan.put("spanOfTrace", PREFIX_TRACE + span.getTraceIdHigh() + span.getTraceIdLow());
			
			//TAG
			List<Tag> tags = span.getTags();
			JSONArray jsonTags = tagsToJson(tags);
			jsonSpan.put("hasSpanTag", jsonTags);
			
			//LOG
			List<Log> logs = span.getLogs();
			JSONArray jsonLogs = logsToJson(logs, id);
			jsonSpan.put("hasLog", jsonLogs);
			
			//REFERENCES
			List<SpanRef> refs = span.getReferences();
			//Put reference properties in jsonSpan object
			refsToJson(refs, jsonSpan);
			
			jsonSpans.add((new JSONObject()).put("Span", jsonSpan));
					
		}
		
		obj.put("Spans", jsonSpans);
		
	}
	
	@SuppressWarnings("unchecked")
	private void refsToJson(List<SpanRef> refs, JSONObject jsonSpan) {
		
		for(SpanRef ref : refs) {
			
			String id = Long.toString(ref.getTraceIdHigh()) + Long.toString(ref.getTraceIdLow()) + Long.toString(ref.getSpanId());
			
			if (ref.refType.equals(SpanRefType.CHILD_OF)) {
				jsonSpan.put("childOf", PREFIX_SPAN + id);
			} else if (ref.refType.equals(SpanRefType.FOLLOWS_FROM)) {
				jsonSpan.put("followsFrom", PREFIX_SPAN + id);
			}
					
		}
	
	}

	@SuppressWarnings("unchecked")
	private JSONArray logsToJson(List<Log> logs, String prefixId) {
		
		JSONArray jsonLogs = new JSONArray();
		
		for(Log log : logs) {
			
			JSONObject jsonLog = new JSONObject();
			jsonLog.put("@id", PREFIX_LOG + prefixId + log.getTimestamp());
			jsonLog.put("timestamp", log.getTimestamp());
			
			List<Tag> fields = log.getFields();
			JSONArray jsonFields = tagsToJson(fields);
			jsonLog.put("hasField", jsonFields);
			
			jsonLogs.add((new JSONObject()).put("Log", jsonLog));
					
		}
		
		return jsonLogs;

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
