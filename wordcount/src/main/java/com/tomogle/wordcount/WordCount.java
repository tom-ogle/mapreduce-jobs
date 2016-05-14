package com.tomogle.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

import static java.lang.String.format;

/**
 * Classic WordCount example.
 * 1st parameter is ignored, 2nd and 3rd app parameters are [in-directory] and [out-directory]
 */
public class WordCount {

  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    String jobName = "My Word Count";

    String inputPath = args[1];
    String outputPath = args[2];

    Configuration configuration = new Configuration();
    Job job = Job.getInstance(configuration, jobName);
    job.setJarByClass(WordCount.class);
    job.setMapperClass(WordCountMap.class);
    job.setCombinerClass(WordCountReduce.class);
    job.setReducerClass(WordCountReduce.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    boolean successfullyCompleted = job.waitForCompletion(true);
    if(successfullyCompleted) {
      System.exit(0);
    } else {
      // TODO: Replace with logger statements
      String historyUrl = job.getHistoryUrl();
      System.out.println(format("Job '%s' failed. See history at: %s", jobName, historyUrl));
      System.exit(1);
    }
  }

  private static class WordCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      while(tokenizer.hasMoreTokens()) {
        String word = tokenizer.nextToken();
        context.write(new Text(word), ONE);
      }
    }
  }

  private static class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int count = 0;
      for (IntWritable value : values) {
        count += value.get();
      }
      context.write(key, new IntWritable(count));
    }
  }
}
