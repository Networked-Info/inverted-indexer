import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.tartarus.snowball.SnowballStemmer;
import org.tartarus.snowball.ext.englishStemmer;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class DocPositionMapper extends Mapper<LongWritable, Text, Text, Text> {

	Set<String> stopwords;

	private Set<?> luceneStopWords = StopAnalyzer.ENGLISH_STOP_WORDS_SET;
	SnowballStemmer stemmer = new englishStemmer();

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

		//title begins after second comma
		int titleIdx = StringUtils.ordinalIndexOf(entry, ",", 2);
		//content begins after third comma
		int contentIdx = StringUtils.ordinalIndexOf(entry, ",", 3);

		String[] title = entry.substring(titleIdx + 1, contentIdx).split("[ \\-—\\/.,;:]");
		for (String word: title) {
			word = processWord(word);
			if (!word.equals("") && !wordPositionList.containsKey(word)) {
				wordPositionList.put(word, new ArrayList<String>());
				wordPositionList.get(word).add("-1");
			}
		}
		String content = entry.substring(contentIdx + 1);

		Matcher m = Pattern.compile("\\w+").matcher(content);
		while (m.find()) {

			String word = processWord(m.group());
			if (!word.equals("")) {
				wordPositionList.putIfAbsent(word, new ArrayList<String>());
				wordPositionList.get(word).add(String.valueOf(m.start()));
			}
		}


		// write the inverted index and position
		for (String word : wordPositionList.keySet()) {
			ArrayList<String> list = wordPositionList.get(word);
			String output = docID + ":[" + String.join(", ", list) + "]";
			context.write(new Text(word), new Text(output));
		}

		wordPositionList.clear();

	}

	private String processWord(String word) {
		//use a regex to retain only unicode latin characters
		word = word.toLowerCase();

		//if word is eliminated or a stopword, return empty string
		if (word.length() < 2 || word.equals("") || stopwords.contains(word) || luceneStopWords.contains(word) || word.matches(".*[^a-zA-Z].*")) {
			return "";
		}

		stemmer.setCurrent(word);
		stemmer.stem();
		word = stemmer.getCurrent();
		return word;
	}


}
