package org.myorg;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PairApproach {

	public static class Map extends
			Mapper<LongWritable, Text, CustomPair, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text item1 = new Text();
		private Text item2 = new Text();
		private CustomPair keyTerm = new CustomPair();
		private CustomPair starTerm = new CustomPair();
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] line = value.toString().split(" ");
			for (int i = 0; i < line.length; i++) {
				String w = line[i];
				if (w == null || w.equals(" ") || w.length() < 2)
					continue;
				item1.set(w);
				int k = 0;
				for (int j = i; j < line.length; j++) {
					String u = line[j];
					if (u == null || u.equals(" ") || u.length() < 2 || k > 1)
						continue;
					if (w.equals(u)) {
						k++;
						continue;
					}
					item2.set(line[j]);
					keyTerm.set(item1, item2);
					context.write(keyTerm, one);
					
					item2.set("*");
					starTerm.set(item1, item2);
					context.write(starTerm, one);
				}
			}

		}
	}

	public static class CustomPair implements WritableComparable<CustomPair> {
		public Text item1;
		public Text item2;

		public CustomPair(Text x, Text y) {
			this.item1 = x;
			this.item2 = y;
		}

		public CustomPair() {
			this.item1 = new Text();
			this.item2 = new Text();
		}

		public void set(Text a, Text b) {
			item1 = a;
			item2 = b;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			item1.readFields(in);
			item2.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			item1.write(out);
			item2.write(out);
		}

		public String toString() {
			return "< " + item1.toString() + ", " + item2.toString() + " >";
		}

		@Override
		public int compareTo(CustomPair o) {
			if (item1.compareTo(o.item1) == 0)
				return (item2.compareTo(o.item2));
			else
				return (item1.compareTo(o.item1));
		}

		@Override
		public boolean equals(Object o) {
			if (o instanceof CustomPair) {
				CustomPair other = (CustomPair) o;
				return item1.equals(other.item1) && item2.equals(other.item2);
			}
			return false;
		}

		@Override
		public int hashCode() {
			return item1.hashCode() + item2.hashCode();
		}
	}

	public static class Reduce extends
			Reducer<CustomPair, IntWritable, CustomPair, DoubleWritable> {
		
		private Text current = new Text();
		int totalCount = 0;
		
		public void reduce(CustomPair key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			if(key.item2.toString().equals("*"))
			{
				if(key.item1.equals(current))
					for (IntWritable val : values) 
						totalCount += val.get();
				
				else
				{
					current.set(key.item1.toString());
					totalCount = 0;
					for (IntWritable val : values) 
						totalCount += val.get();
				}
			}
			else
			{
				int sum = 0;
				for (IntWritable val : values) 
					sum += val.get();
				double val = (double) sum / (double) totalCount;
				context.write(key, new DoubleWritable(val));
			}
			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "pairapproach");
		job.setJarByClass(PairApproach.class);

		job.setOutputKeyClass(CustomPair.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapOutputKeyClass(CustomPair.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}