package org.myorg;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AvgCompute {

	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private Text word = new Text();
		

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] l1 = value.toString().split(" ");
			String first = l1[0];
			String last = l1[l1.length-1];
			if (last.contains(".") || last.contains(" ") || last.contains("'")
					|| last.contains("?") || last.contains("-")
					|| last.contains("_") || last.contains(":"))
				return;
			word.set(first);
			context.write(word, new IntWritable(Integer.parseInt(last)));
		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, FloatWritable> {
		private FloatWritable result = new FloatWritable();
		Float average = 0f;
		Float count = 0f;
		int sum = 0;

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			for (IntWritable val : values) {
				sum += val.get();
			}
			count += 1;
			average = sum / count;
			result.set(average);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "avgcompute");
		job.setJarByClass(AvgCompute.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}