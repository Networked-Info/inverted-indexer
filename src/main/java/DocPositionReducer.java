import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

public class DocPositionReducer extends Reducer<Text, Text, NullWritable, Text>{
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		
		JSONObject result = new JSONObject();
		Map<String, String> docPositionMap = new HashMap<>();
		TreeMap<Integer, List<String>> rankMap = new TreeMap<Integer, List<String>>();
		
		for (Text indexAndPosition : values) {
			String entry = indexAndPosition.toString();
			String docID = entry.substring(0, entry.indexOf(":"));
			String positions = entry.substring(entry.indexOf("["));
			if (!docPositionMap.containsKey(docID)) {
				docPositionMap.put(docID, positions);
			}
			
			int count = positions.split(",").length;
			if (positions.contains("-1")) 
				count += 100;
			rankMap.putIfAbsent(count, new ArrayList<String>());
			rankMap.get(count).add(docID);
		}
		
		JSONArray ja = new JSONArray();
		for (int count: rankMap.descendingKeySet()) {
			for (String docID: rankMap.get(count)) {
				JSONObject obj = new JSONObject();
				obj.put(docID, docPositionMap.get(docID));
				ja.add(obj);
			}
		}
		
		result.put(key.toString(), ja);
		
		context.write(NullWritable.get(), new Text(result.toString()));
		
		docPositionMap.clear();
		ja.clear();
	}

}
