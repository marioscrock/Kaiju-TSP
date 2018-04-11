package collector;

import java.util.List;
import java.util.logging.Logger;

import org.apache.thrift.TException;
import org.eclipse.jetty.util.ConcurrentHashSet;
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
import websocket.JsonTraces;

public class CollectorHandler extends ThriftRequestHandler<Collector.submitBatches_args, Collector.submitBatches_result> implements Collector.Iface{
	
	public ConcurrentHashSet<thriftgen.Process> processesSeen = new ConcurrentHashSet<thriftgen.Process>();
	//Keep track of traces already seen
	public ConcurrentHashSet<String> traceIds = new ConcurrentHashSet<String>();
	public int numbBatches = 0;
	public static final String PREFIX_LOG = "tr:log_";
	public static final String PREFIX_PROCESS = "tr:process_";
	public static final String PREFIX_SPAN = "tr:span_";
	public static final String PREFIX_TAG = "tr:tag_";
	public static final String PREFIX_TRACE = "tr:trace_";
	
	@Override
	public List<BatchSubmitResponse> submitBatches(List<Batch> batches) throws TException {
		
		Logger log = Logger.getLogger("jaeger-java-collector");
		
		log.info("Processing batch number: " + numbBatches + "\nBatch size: " + batches.size());
		
		numbBatches += batches.size();
		
		JSONObject obj = new JSONObject();
		
		for(Batch batch : batches) {
			try {
				batchToJson(obj, batch);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		//log.info(obj.toJSONString());
		JsonTraces.sendBatch(obj);
		
//        try (FileWriter file = new FileWriter("/Users/Mario/Desktop/test" + numbBatches + ".json")) {
//
//            file.write(obj.toJSONString());
//            file.flush();
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        System.out.print(obj);
		
		return null;
		
	}
	
	
	@SuppressWarnings("unchecked")
	private void batchToJson(JSONObject obj, Batch batch) throws Exception {
		
		thriftgen.Process process = batch.getProcess();
		//Get processId related to the process
		int processId = hashProcess(process);
		
		boolean seen = false;
		if (processesSeen != null) {
			
			for (thriftgen.Process p : processesSeen)
				if (equalsProcess(process, p)) {
					
					seen = true;
					break;
					
				} else if (hashProcess(process) == hashProcess(p)){
					//If not equal as defined in equalsProcess check for colliding hashes
					//TODO Specialize the exception type 
					throw new Exception("Colliding hash");
				}
				
		}
		
		if (!seen) {
			
			//Generate Id by custom hash function and add process if NOT ALREADY SEEN
			processId = hashProcess(process);
			processesSeen.add(process);	
				
			//Add Tags of the process
			JSONArray jsonTags = tagsToJson(process.getTags());
				
			for (Object o : jsonTags) {
				JSONObject jsonTag = (JSONObject) o;
				obj.put("Tag", jsonTag.get("Tag"));
			}
			
			//PROCESS
			//Add JSONLD representation of process to obj
			processToJson(obj, process, Integer.toString(processId)); 
					
		}
		
		//TRACES Add all traces to obj
		//Map process as related to a given trace in traceIds map
		for (Span span : batch.getSpans()) {
			
			String traceIdHex = traceIdToHex(span.getTraceIdHigh(),span.getTraceIdLow());
			
			if (!traceIds.contains(traceIdHex)) {
				
				traceIds.add(traceIdHex);
				
				//Trace to JSONLD
				JSONObject jsonTrace = new JSONObject();
				jsonTrace.put("@id", PREFIX_TRACE + traceIdHex);
				jsonTrace.put("traceId", traceIdHex);
				
				obj.put("Trace", jsonTrace);
				
			} 
			
		}
			
		//SPANS
		List<Span> spans = batch.getSpans();
		spansToJson(obj, spans, Integer.toString(processId));  //Add JSONLD representation of spans to obj
		
	}
	
	@SuppressWarnings("unchecked")
	private void processToJson(JSONObject obj, thriftgen.Process process, String processId) {
		
		JSONObject jsonProcess = new JSONObject();
		jsonProcess.put("@id", PREFIX_PROCESS + processId);
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
			
			jsonProcess.put("hasProcessTag", PREFIX_TAG + r(id));
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
			
			jsonTag.put("@id", PREFIX_TAG + r(id));
			
			JSONObject jsonTagNode = new JSONObject();
			jsonTagNode.put("Tag", jsonTag);
			jsonTags.add(jsonTagNode);
					
		}
		
		return jsonTags;
		
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
			
			JSONObject jsonLogNode = new JSONObject();
			jsonLogNode.put("Log", jsonLog);
			jsonLogs.add(jsonLogNode);
					
		}
		
		return jsonLogs;

	}

	@SuppressWarnings("unchecked")
	private void spansToJson(JSONObject obj, List<Span> spans, String processId) {
		
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
			jsonSpan.put("spanOfProcess", PREFIX_PROCESS + processId);
			jsonSpan.put("spanOfTrace", PREFIX_TRACE + traceIdHex);
			
			//TAG
			List<Tag> tags = span.getTags();
			if (tags != null) { 
				JSONArray jsonTags = tagsToJson(tags);
				jsonSpan.put("hasSpanTag", jsonTags);
			}
			
			//LOG
			List<Log> logs = span.getLogs();
			if (logs != null) { 
				JSONArray jsonLogs = logsToJson(logs, id);
				jsonSpan.put("hasLog", jsonLogs);
			}
			
			//REFERENCES
			List<SpanRef> refs = span.getReferences();
			if (refs != null) {
				//Put reference properties in jsonSpan object
				refsToJson(refs, jsonSpan);
			}
			
			//CHILDOF reference with Parent
			if (span.getParentSpanId() != 0) {
				jsonSpan.put("childOf", PREFIX_SPAN + traceIdHex + Long.toHexString(span.getParentSpanId()));
			}
			
			obj.put("Span", jsonSpan);
					
		}
		
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
	
	//Special char replace
	private String r(String string) {
		
		String rString = string;
		rString = rString.replace(" ", "%20")
				.replace("!", "%21")
				.replace("''", "%22")
				.replace("#", "%23")
				.replace("$", "%24")
				.replace("&", "%26")
				.replace(")", "%29")
				.replace("*", "%2A")
				.replace("+", "%2B")
				.replace(",", "%2C")
				.replace("/", "%2F")
				.replace(":", "%3A")
				.replace(";", "%3B")
				.replace("=", "%3D")
				.replace("?", "%3F")
				.replace("@", "%40")
				.replace("[", "%5B")
				.replace("]", "%5D");
		
		return rString;
	
	}
	
	
	/**
	 * Returns True if two processes are equal, processes are equals if same serviceName
	 * and same tags(despite of their order).
	 * @param process1 First process to be compared
	 * @param process2 Second process to be compared
	 * @return True if two processes are equal, processes are equals if same serviceName
	 * and same tags(despite of their order). False otherwise.
	 */
	private boolean equalsProcess(thriftgen.Process process1, thriftgen.Process process2) {
			
	    if (process1 == process2)
	      return true;

	    boolean process1_present_serviceName = true && process1.isSetServiceName();
	    boolean process2_present_serviceName = true && process2.isSetServiceName();
	    if (process1_present_serviceName || process2_present_serviceName) {
	      if (!(process1_present_serviceName && process2_present_serviceName))
	        return false;
	      if (!process1.serviceName.equals(process2.serviceName))
	        return false;
	    }

	    boolean process1_present_tags = true && process1.isSetTags();
	    boolean process2_present_tags = true && process2.isSetTags();
	    if (process1_present_tags || process2_present_tags) {
	      if (!(process1_present_tags && process2_present_tags))
	        return false;
	      
	      for (Tag tag : process1.getTags())
	    	  if (!process2.getTags().contains(tag))
	    	  	return false;
	      
	      for (Tag tag : process2.getTags())
	    	  if (!process1.getTags().contains(tag))
	    	  	return false;
	      
	    }

	    return true;
		
	}
	
	//Provides an hash for each Process, same hash if same serviceName
	//and same tags(despite of their order)
	private int hashProcess(thriftgen.Process process) {
		
		int result = 0;
		//37 - must be prime
		result = 37 * result + process.getServiceName().hashCode();
		
		int tagsHash = 0;
		if (process.getTags() != null) {
			for (Tag tag : process.getTags())
				tagsHash += tag.hashCode();
		}
		result = 37 * result + tagsHash;
		
		return result;
		
	}

	@Override
	public ThriftResponse<Collector.submitBatches_result> handleImpl(ThriftRequest<Collector.submitBatches_args> request) {
		
		List<Batch> batches = request.getBody(Collector.submitBatches_args.class).getBatches();
		
		Thread t = new Thread(new Runnable() {
				
			@Override
			public void run() {
				try {
					submitBatches(batches);
				} catch (TException e) {
					e.printStackTrace();
				}		
			}
			
		});
		
		t.run();
		
		return new ThriftResponse.Builder<Collector.submitBatches_result>(request)
	            .setBody(new Collector.submitBatches_result())
	            .build();
		
	}

}
