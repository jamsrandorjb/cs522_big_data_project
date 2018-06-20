package org.myorg;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.MapWritable;


public class StripesApproach {

	public static class Map extends Mapper<LongWritable, Text, Text, MapWritable> {
		public Text keyTerm = new Text();
		public Text item1 = new Text();
		public IntWritable item2 = new IntWritable(1);
		MapWritable stripe = new MapWritable();
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] line = value.toString().split(" ");
			
			for (int i = 0; i < line.length; i++) {
				
				String w = line[i];
				if (w == null || w.equals(" ") || w.length() < 2)
					continue;
				
				stripe.clear();
				keyTerm.set(w);
				int k = 0;
				for (int j = i; j < line.length; j++) {
					String u = line[j];
					if (u == null || u.equals(" ") || u.length() < 2 || k > 1)
						continue;
					if (w.equals(u)) {
						k++;
						continue;
					}
					item1.set(u);
					if(stripe.containsKey(item1)){
						IntWritable cnt = (IntWritable)stripe.get(item1);
						cnt.set(cnt.get()+1);
					}
					else
						stripe.put(item1, item2);
				}
				if (!stripe.isEmpty())
					context.write(keyTerm, stripe);

			}

		}
		
	}
	

	public static class Reduce extends
			Reducer<Text, MapWritable, Text, Text> {

		int totalCount = 0;
		private MapWritable sumofAllStripes = new MapWritable();

		public void reduce(Text key, Iterable<MapWritable> values,
				Context context) throws IOException, InterruptedException {
			sumofAllStripes.clear();
			for(MapWritable value : values)
				addStripToResult(value);
			
			int totalCount = 0;
			
			for(Entry<Writable, Writable> e : sumofAllStripes.entrySet())
				totalCount += Integer.parseInt(e.getValue().toString());
			
			DecimalFormat decimalFormat = new DecimalFormat("0.00");
			StringBuilder str = new StringBuilder();
			for(Entry<Writable, Writable> e : sumofAllStripes.entrySet())
			{
				if(str.length()>0)
					str.append(", ");
				str.append("(")
					.append(e.getKey())
					.append(", ")
					.append(decimalFormat.format(Integer.parseInt(e.getValue().toString())/(double) totalCount))
					.append(") ");
			}
			context.write(key, new Text("[ "+str.toString()+" ]"));		
		}
		private void addStripToResult(MapWritable m){
			for(Writable k : m.keySet())
			{
				IntWritable count = (IntWritable) m.get(k);
				if(sumofAllStripes.containsKey(k))
				{
					IntWritable resutl = (IntWritable) sumofAllStripes.get(k);
					resutl.set(resutl.get() + count.get());
				}
				else
					sumofAllStripes.put(k, count);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "stripesapproach");
		job.setJarByClass(StripesApproach.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}