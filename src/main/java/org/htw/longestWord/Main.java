package org.htw.longestWord;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.*;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Longest word");

        // set main class for the jar:
        job.setJarByClass(Main.class);

        // set the mapper and reducer classes
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // output types (key, value)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // set input/output paths:
        FileInputFormat.addInputPath(job, new Path("/Users/elias/Documents/Uni/HTW/MA/ProgAlg/HadoopWordCount/src/main/resources/input/*"));

        //  check if exists:
        FileSystem fs = FileSystem.get(conf);

        Path outDir = new Path("/Users/elias/Documents/Uni/HTW/MA/ProgAlg/HadoopWordCount/src/main/resources/output");

        if (fs.exists(outDir)) {
            System.out.println("Already exists.. Overwriting");
            fs.delete(outDir, true);
        }
        FileOutputFormat.setOutputPath(job, outDir);

        // run the job
        try {
            long startTime = System.currentTimeMillis();
            job.waitForCompletion(true);
            long elapsedTime = System.currentTimeMillis() - startTime;
            System.out.println("Total execution time: " + elapsedTime + " ms");
        } catch (InterruptedException | ClassNotFoundException | IOException e) {
            e.printStackTrace();
        }
        System.out.println("Done!");
    }
}