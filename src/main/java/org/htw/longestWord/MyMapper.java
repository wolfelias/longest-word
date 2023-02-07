package org.htw.longestWord;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString();
        String language = new Path(filePathString).getParent().getName().toLowerCase();
        String[] tokens = value.toString().split("\\P{L}+");

        for (String token : tokens) {
            context.write(new Text(language + "_" + token), new IntWritable(token.length()));
        }
    }
}
