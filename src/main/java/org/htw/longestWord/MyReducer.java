package org.htw.longestWord;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.*;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final HashMap<String, HashMap<String, Integer>> allData = new HashMap<>();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) {

        String[] languageKey = key.toString().split("_");
        if (languageKey.length > 1) {
            String language = languageKey[0];
            String word = languageKey[1];
            int wordLength = values.iterator().next().get();

            if (allData.containsKey(language)) {
                allData.get(language).put(word, wordLength);
            } else {
                HashMap<String, Integer> tempMap = new HashMap<>();
                tempMap.put(word, wordLength);
                allData.put(language, tempMap);
            }
        }
    }

    /**
     * This cleanup method is used for sorting the allData HashMap and write the context
     */
    @Override
    protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        Comparator<Map.Entry<String, Integer>> comparator = (o1, o2) -> o2.getValue().compareTo(o1.getValue());


        for (Map.Entry<String, HashMap<String, Integer>> language : allData.entrySet()) {
            StringBuilder sb = new StringBuilder();

            List<Map.Entry<String, Integer>> values = new ArrayList<>(language.getValue().entrySet());
            values.sort(comparator);
            sb.append(language.getKey());
            sb.append(" - ");
            sb.append(values.get(0).getKey());
            context.write(new Text(sb.toString()), new IntWritable(values.get(0).getValue()));
        }
    }
}
