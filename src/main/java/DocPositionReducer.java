import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

public class DocPositionReducer extends Reducer<Text, Text, Text, Text>{
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		
		Map<String, String> docPositionMap = new HashMap<>();
		System.out.println("weird");
		for (Text indexAndPosition : values) {
			String entry = indexAndPosition.toString();
			String docID = entry.substring(0, entry.indexOf(":"));
			String positions = entry.substring(entry.indexOf("["));
			if (!docPositionMap.containsKey(docID)) {
				docPositionMap.put(docID, positions);
			}
		}
		
		ArrayList<String> list = new ArrayList<>();
		for (String docID : docPositionMap.keySet()) {
			list.add("{" + docID + ":" + docPositionMap.get(docID) + "}");
		}
		
		context.write(new Text(key + "=>"), new Text(String.join(",", list)));
		
		docPositionMap.clear();
		list.clear();
	}

}
