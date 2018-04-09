package collector;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

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
	public HashMap<thriftgen.Process, UUID> processesMap = new HashMap<thriftgen.Process, UUID>();
	//For each trace keep track of processes related in an HashMap
	//For each trace the HashMap contains (processHash, incremental processId for given trace) pairs
	public HashMap<String, HashMap<UUID, Integer>> traceIds = new HashMap<String, HashMap<UUID, Integer>>();
	public int numbBatches = 0;
	public static final String PREFIX_LOG = "tr:log/";
	public static final String PREFIX_PROCESS = "tr:process/";
	public static final String PREFIX_SPAN = "tr:span/";
	public static final String PREFIX_TAG = "tr:tag/";
	public static final String PREFIX_TRACE = "tr:trace/";
	
	@Override
	public List<BatchSubmitResponse> submitBatches(List<Batch> batches) throws TException {
		
		System.out.println("Processing batch number: " + numbBatches + "\nBatch size: " + batches.size());
		numbBatches += batches.size();
		
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
	
	
	@SuppressWarnings("unchecked")
	private void batchToJson(JSONObject obj, Batch batch) {
		
		thriftgen.Process process = batch.getProcess();
		UUID processUUID;
		
		if (!processesMap.keySet().contains(process)) {
			
			//Generate UUID and add process if NOT ALREADY SEEN
			processUUID = UUID.randomUUID();
			processesMap.put(process, processUUID);	
			
			//Add Tags of the process
			JSONArray jsonTags = tagsToJson(process.getTags());
			for (Object o : jsonTags) {
				JSONObject jsonTag = (JSONObject) o;
				obj.put("Tag", jsonTag.get("Tag"));
			}
			
		} else {
			
			//Get processUUID related to the process
			processUUID = processesMap.get(process);	
		}
		
		//TRACES Add all traces to obj
		//Map process as related to a given trace in traceIds map
		for (Span span : batch.getSpans()) {
			
			String traceIdHex = traceIdToHex(span.getTraceIdHigh(),span.getTraceIdLow());
			
			if (!traceIds.keySet().contains(traceIdHex)) {
				
				traceIds.put(traceIdHex, new HashMap<UUID, Integer>());
				
				//Trace to JSONLD
				JSONObject jsonTrace = new JSONObject();
				jsonTrace.put("@id", PREFIX_TRACE + traceIdHex);
				jsonTrace.put("traceId", traceIdHex);
				
				obj.put("Trace", jsonTrace);
				
			} 
			
			HashMap<UUID, Integer> mapTraceProcess = traceIds.get(traceIdHex);
			//If process not already mapped for a given trace
			if (!mapTraceProcess.containsKey(processUUID)) {
				
				//Add process UUID to the map trace-processes
				int processTraceId = mapTraceProcess.keySet().size();
				mapTraceProcess.put(processUUID, processTraceId);
				
				//PROCESS
				//Add JSONLD representation of process to obj
				processToJson(obj, process, traceIdHex + "p" + processTraceId); 
				
			} 
			
		}
			
		//SPANS
		List<Span> spans = batch.getSpans();
		spansToJson(obj, spans, processUUID);  //Add JSONLD representation of spans to obj
		
	}
	
	@SuppressWarnings("unchecked")
	private void processToJson(JSONObject obj, thriftgen.Process process, String postfix_process) {
		
		//TODO ADD id to the PROCESS! Scan all traces and add all ids traceId+processId? 
		//We DO NOT have process Ids at this point! Review Process definition!
		JSONObject jsonProcess = new JSONObject();
		jsonProcess.put("@id", PREFIX_PROCESS + postfix_process);
		jsonProcess.put("serviceName", process.getServiceName());
		
		//We assume tags created once for process in batchToJson function
		List<Tag> tags = process.getTags();
		for(Tag tag : tags) {
			
			String id = tag.getKey();
			
			if (tag.vType.equals(TagType.STRING))
				id += tag.vStr;
			else if (tag.vType.equals(TagType.DOUBLE))
				id += tag.vDouble;
			else if (tag.vType.equals(TagType.BOOL))
				id += tag.vBool;
			else if (tag.vType.equals(TagType.LONG))
				id += tag.vLong;
			else if (tag.vType.equals(TagType.BINARY))
				id += tag.vBinary;
			
			jsonProcess.put("hasProcessTag", PREFIX_TAG + id);
		}
		
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
			else if (tag.vType.equals(TagType.DOUBLE)){
				jsonTag.put("doubleVal", tag.vDouble);
				id += tag.vDouble;
			}
			else if (tag.vType.equals(TagType.BOOL)){
				jsonTag.put("boolVal", tag.vBool);
				id += tag.vBool;
			}
			else if (tag.vType.equals(TagType.LONG)){
				jsonTag.put("longVal", tag.vLong);
				id += tag.vLong;
			}
			else if (tag.vType.equals(TagType.BINARY)){
				jsonTag.put("binaryVal", tag.vBinary);
				id += tag.vBinary;
			}
			
			jsonTag.put("@id", PREFIX_TAG + id);
			jsonTags.add((new JSONObject()).put("Tag", jsonTag));
					
		}
		
		return jsonTags;
		
	}

	@SuppressWarnings("unchecked")
	private void spansToJson(JSONObject obj, List<Span> spans, UUID processUUID) {
		
		JSONArray jsonSpans = new JSONArray();
		
		for(Span span : spans) {
			
			JSONObject jsonSpan = new JSONObject();
			
			//ATTENTION: convert in HEX like DB
			String traceIdHex = traceIdToHex(span.getTraceIdHigh(), span.getTraceIdLow());
			String spanIdHex = Long.toHexString(span.getSpanId());
			String id = traceIdHex + spanIdHex;
			
			//Data properties
			jsonSpan.put("@id", PREFIX_SPAN + id);
			jsonSpan.put("spanId", spanIdHex);
			jsonSpan.put("operationName", span.getOperationName());
			jsonSpan.put("startTime", span.getStartTime());
			jsonSpan.put("duration", span.getDuration());
			jsonSpan.put("flags", span.getFlags());
			
			//Object properties
			jsonSpan.put("spanOfProcess", PREFIX_PROCESS + traceIdHex + "p" + traceIds.get(traceIdHex).get(processUUID));
			jsonSpan.put("spanOfTrace", PREFIX_TRACE + traceIdHex);
			
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
	
	private String traceIdToHex(Long traceIdHigh, Long traceIdLow) {
		
		return Long.toHexString(Long.valueOf((Long.toString(traceIdHigh) 
				+ Long.toString(traceIdLow))).longValue());
		
	}
	
	@SuppressWarnings("unchecked")
	private void refsToJson(List<SpanRef> refs, JSONObject jsonSpan) {
		
		for(SpanRef ref : refs) {
			
			String id = traceIdToHex(ref.getTraceIdHigh(), ref.getTraceIdLow()) + Long.toString(ref.getSpanId());
			
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
