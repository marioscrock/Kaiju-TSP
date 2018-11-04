package websocket;

import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import thriftgen.Batch;
import thriftgen.Log;
import thriftgen.Span;
import thriftgen.SpanRef;
import thriftgen.SpanRefType;
import thriftgen.Tag;
import thriftgen.TagType;

/** 
 * Class offering a static method to serialize a {@link thriftgen.Batch Batch} object 
 * to a {@link org.json.simple.JSONObject JSONObject} expandable through the {@code tracing_ontology_context.json} context.
 * @author Mario
 *
 */
public class JsonLDSerialize {
	
	public static final String PREFIX_LOG = "tr:log_";
	public static final String PREFIX_PROCESS = "tr:process_";
	public static final String PREFIX_SPAN = "tr:span_";
	public static final String PREFIX_TAG = "tr:tag_";
	public static final String PREFIX_TRACE = "tr:trace_";
	
	/**
	 * Static method to serialize a {@link thriftgen.Batch Batch} object 
	 * to a {@link org.json.simple.JSONObject JSONObject} expandable through the {@code tracing_ontology_context.json} file
	 * @param batch The batch to serialize
	 * @return JSONObject representing the batch given as input
	 * @throws Exception If errors while processing the batch
	 */
	@SuppressWarnings("unchecked")
	public static JSONObject batchToJson(Batch batch) throws Exception {
		
		JSONObject obj = new JSONObject();
		
		thriftgen.Process process = batch.getProcess();
		
		//Get processId related to the process
		int processId = hashProcess(process);
		
		//Add JSONLD representation of process to obj
		JSONObject jsonProcess = processToJson(process, Integer.toString(processId), true); 
		obj.put("Process", jsonProcess);
		
		if (batch.getSpans() != null) {
				
			//SPANS
			List<Span> spans = batch.getSpans();
			JSONArray jsonSpans = spansToJson(spans, Integer.toString(processId));  //Add JSONLD representation of spans to obj
			jsonProcess.put("hasSpan",jsonSpans);
			
		}
		
		return obj;
		
	}
	
	/**
	 * Return a JSONObject representing a {@link thriftgen.Process Process} object
	 * @param process The process to serialize
	 * @param processId The {@code @id} assigned to the process
	 * @param addTags {@code True} if process tags must be serialized
	 * @return JSONObject representing the process given as input
	 */
	@SuppressWarnings("unchecked")
	private static JSONObject processToJson(thriftgen.Process process, String processId, boolean addTags) {
		
		JSONObject jsonProcess = new JSONObject();
		jsonProcess.put("@id", PREFIX_PROCESS + processId);
		jsonProcess.put("serviceName", process.getServiceName());
		
		if (addTags) {
			
			JSONArray jsonTags = tagsToJson(process.getTags());	
			jsonProcess.put("hasProcessTag", jsonTags);
			
		} else {
		
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
		}
		
		return jsonProcess;

	}
	
	/**
	 * Return a {@link org.json.simple.JSONArray JSONArray} representing a list of {@link thriftgen.Tag Tag} objects
	 * @param process The tags to serialize
	 * @return JSONArray representing the tags given as input
	 */
	@SuppressWarnings("unchecked")
	private static JSONArray tagsToJson(List<Tag> tags) {
		
		JSONArray jsonTags = new JSONArray();
		
		for(Tag tag : tags) {
			
			JSONObject jsonTag = new JSONObject();
			String id = tag.getKey();
			jsonTag.put("tagKey", tag.getKey());
			
			if (tag.getVType().equals(TagType.STRING)){
				jsonTag.put("stringVal", tag.getVStr());
				id += tag.getVStr();
			}
			else if (tag.getVType().equals(TagType.DOUBLE)){
				jsonTag.put("doubleVal", tag.getVDouble());
				id += tag.getVDouble();
			}
			else if (tag.getVType().equals(TagType.BOOL)){
				jsonTag.put("boolVal", tag.isVBool());
				id += tag.isVBool();
			}
			else if (tag.getVType().equals(TagType.LONG)){
				jsonTag.put("longVal", tag.getVLong());
				id += tag.getVLong();
			}
			else if (tag.getVType().equals(TagType.BINARY)){
				jsonTag.put("binaryVal", tag.getVBinary());
				id += tag.getVBinary();
			}
			
			jsonTag.put("@id", PREFIX_TAG + r(id));
			jsonTag.put("@type", "Tag");
		
			jsonTags.add(jsonTag);
					
		}
		
		return jsonTags;
		
	}
	
	/**
	 * Return a {@link org.json.simple.JSONArray JSONArray} representing a list of {@link thriftgen.Log Log} objects
	 * @param logs The list of logs to serialize
	 * @param prefixId The prefix related to the span for the {@code @id} assigned to logs
	 * @return JSONArray representing the logs given as input
	 */
	@SuppressWarnings("unchecked")
	private static JSONArray logsToJson(List<Log> logs, String prefixId) {
		
		JSONArray jsonLogs = new JSONArray();
		
		for(Log log : logs) {
			
			JSONObject jsonLog = new JSONObject();
			jsonLog.put("@id", PREFIX_LOG + prefixId + log.getTimestamp());
			jsonLog.put("@type", "Log");
			jsonLog.put("timestamp", log.getTimestamp());
			
			List<Tag> fields = log.getFields();
			JSONArray jsonFields = tagsToJson(fields);
			jsonLog.put("hasField", jsonFields);
			
			jsonLogs.add(jsonLog);
					
		}
		
		return jsonLogs;

	}
	
	/**
	 * Return a {@link org.json.simple.JSONArray JSONArray} representing a list of {@link thriftgen.Span Span} objects
	 * @param logs The list of spans to serialize
	 * @param prefixId The {@code @id} assigned to the process related to the spans
	 * @return JSONArray representing the spans given as input
	 */
	@SuppressWarnings("unchecked")
	private static JSONArray spansToJson(List<Span> spans, String processId) {
		
		JSONArray jsonSpans = new JSONArray();
		
		for(Span span : spans) {
			
			JSONObject jsonSpan = new JSONObject();
			
			//ATTENTION: converting in HEX
			String traceIdHex = traceIdToHex(span.getTraceIdHigh(), span.getTraceIdLow());
			String spanIdHex = Long.toHexString(span.getSpanId());
			String id = traceIdHex + spanIdHex;
			
			//Data properties
			jsonSpan.put("@id", PREFIX_SPAN + id);
			jsonSpan.put("@type", "Span");
						
			jsonSpan.put("spanId", spanIdHex);
			jsonSpan.put("operationName", span.getOperationName());
			jsonSpan.put("startTime", span.getStartTime());
			jsonSpan.put("duration", span.getDuration());
			jsonSpan.put("flags", span.getFlags());
			
			//Object properties
			
			//TRACE
			JSONObject jsonTrace = new JSONObject();
			jsonTrace.put("@id", PREFIX_TRACE + traceIdHex);
			jsonTrace.put("@type", "Trace");
			jsonTrace.put("traceId", traceIdHex);
			
			jsonSpan.put("spanOfTrace", jsonTrace);
			
			//PROCESS
			jsonSpan.put("spanOfProcess", PREFIX_PROCESS + processId);
			
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
				refsToJson(span, jsonSpan, refs);
			}
			
			jsonSpans.add(jsonSpan);
					
		}
		
		return jsonSpans;
		
	}
	
	/**
	 * Convert {@link thriftgen.Span#traceIdHigh traceIdHigh} and {@link thriftgen.Span#traceIdLow traceIdLow}
	 * to their HEX representation.
	 * @param traceIdHigh
	 * @param traceIdLow
	 * @return String containing the HEX value 
	 */
	public static String traceIdToHex(Long traceIdHigh, Long traceIdLow) {
		
		if (traceIdHigh == 0)
			return String.format("%x", traceIdLow);
		else
			return String.format("%x%016x", traceIdHigh, traceIdLow);
		
	}
	
	/**
	 * Add to a JSONObject representing a {@link thriftgen.Span Span} a JSON representation of {@link thriftgen.SpanRef SpanRef} objects
	 * @param span The span related to the references
	 * @parma JSONObject JSONObject representing the span
	 * @param refs The list of refs to serialize
	 */
	@SuppressWarnings("unchecked")
	private static void refsToJson(Span span, JSONObject jsonSpan, List<SpanRef> refs) {
		
		JSONArray childOf = new JSONArray();
		JSONArray followsFrom = new JSONArray();
		
		for(SpanRef ref : refs) {
			
			String id = traceIdToHex(ref.getTraceIdHigh(), ref.getTraceIdLow()) + Long.toString(ref.getSpanId());
			
			if (ref.refType.equals(SpanRefType.CHILD_OF))
				childOf.add(PREFIX_SPAN + id);
			else if (ref.refType.equals(SpanRefType.FOLLOWS_FROM))
				followsFrom.add(PREFIX_SPAN + id);
		
		}
		
		//Add CHILDOF reference with Parent
		if (span.getParentSpanId() != 0L) {
			childOf.add(PREFIX_SPAN + traceIdToHex(span.getTraceIdHigh(), span.getTraceIdLow()) +
					Long.toHexString(span.getParentSpanId()));
		}
		
		if(childOf.size() > 0)
			jsonSpan.put("childOf", childOf);
		
		if(followsFrom.size() > 0)
			jsonSpan.put("followsFrom", followsFrom);
	
	}
	
	//Special chars replace
	private static String r(String string) {
		
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
	public static boolean equalsProcess(thriftgen.Process process1, thriftgen.Process process2) {
			
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
	public static int hashProcess(thriftgen.Process process) {
		
		int result = 0;
		//37 - must be prime
		result = 37 * result + process.getServiceName().hashCode();
		
		int tagsHash = 0;
		if (process.getTags() != null) {
			for (Tag tag : process.getTags())
				tagsHash += tag.hashCode();
		}
		result = 37 * result + tagsHash;
		
		return Math.abs(result); 
		
	}

}
