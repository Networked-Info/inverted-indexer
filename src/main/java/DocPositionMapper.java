import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.http.util.TextUtils;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

public class DocPositionMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	Set<String> stopwords;
	
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		String stopwordsJSON = conf.get("stopwords");
		Gson gson = new Gson();
		
		Type swSetType = new TypeToken<HashSet<String>>() {}.getType();
		stopwords = gson.fromJson(stopwordsJSON, swSetType);
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String entry = value.toString();
		Map<String, ArrayList<String>> wordPositionList = new HashMap<>();
		
		//grab the docID from before first comma
		String docID = entry.substring(0, entry.indexOf(","));

		//content begins after third comma
		int contentIdx = StringUtils.ordinalIndexOf(entry, ",", 3);
		String[] contentArr = entry.substring(contentIdx + 1).split("[ \\-—\\/.,;:]");
		List<String> content = new ArrayList<String>(Arrays.asList(contentArr));

		// record the position for each word
		for (int i = 0; i < content.size(); i++) {
			String word = content.get(i);
			word = processWord(word);
			
			// if word is not stopword record the position and docId
			if (!word.equals("")) {
				ArrayList<String> list = wordPositionList.containsKey(word)? wordPositionList.get(word) : new ArrayList<String>();
				list.add(Integer.toString(i));
				wordPositionList.put(word, list);
			}
		}
		
		// write the inverted index and position
		for (String word : wordPositionList.keySet()) {
			ArrayList<String> list = wordPositionList.get(word);
			String output = docID + ":[" + String.join(", ", list) + "]";
			context.write(new Text(word), new Text(output));
		}
		
		wordPositionList.clear();
		content.clear();
		
	}
	
	private String processWord(String word) {
		//use a regex to retain only unicode latin characters
		word = word.toLowerCase();

		//if word is eliminated or a stopword, return empty string
		if (word.equals("") || stopwords.contains(word) || word.matches(".*[^a-zA-Z].*")) {
			return "";
		}

		return word;
	}


}