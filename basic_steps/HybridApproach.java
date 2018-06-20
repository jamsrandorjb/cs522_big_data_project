package org.myorg;

import java.io.DataInput;
import java.io.DataOutput;
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


public class HybridApproach {

	public static class Map extends Mapper<LongWritable, Text, CustomPair, IntWritable> {
		
		MapWritable map = new MapWritable();
		private final static IntWritable one = new IntWritable(1);
		private Text item1 = new Text();
		private Text item2 = new Text();
		private CustomPair keyTerm = new CustomPair();
		
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
					if(!map.containsKey(keyTerm))
						map.put(keyTerm, one);
					else
						map.put(keyTerm,  new IntWritable(Integer.parseInt(map.get(keyTerm).toString()) + 1));
					
				}
				for(Entry<Writable, Writable> e : map.entrySet())
					context.write((CustomPair)e.getKey(), (IntWritable)e.getValue());
				
				
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
			Reducer<CustomPair, IntWritable, Text, Text> {

		private long total =0;
		private String prevKeyFirst = null;
		private MapWritable map = new MapWritable();  
		
				
		public void reduce(CustomPair key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			
			
			String curKeyFirst = key.item1.toString();
			String curKeySecond = key.item2.toString();
			
			if(prevKeyFirst != null && !prevKeyFirst.equals(curKeyFirst))
			{
				DecimalFormat decimalFormat = new DecimalFormat("0.00");
				StringBuilder str = new StringBuilder();
				for(Entry<Writable, Writable> e : map.entrySet())
				{
					if(str.length()>0)
						str.append(", ");
					str.append("(")
						.append(e.getKey())
						.append(", ")
						.append(decimalFormat.format(Integer.parseInt(e.getValue().toString())/(double) total))
						.append(") ");
				}
				context.write(new Text(prevKeyFirst), new Text("[ "+str.toString()+" ]"));
				
				total = 0;
				prevKeyFirst = curKeyFirst;
				map = new MapWritable(); 
			}
			float sum =0;
			
			for(IntWritable i : values)
			{
				IntWritable oldVal = (IntWritable) map.get(key.item2);
				if(oldVal != null)
					map.put(new Text(curKeySecond), new IntWritable( i.get() + oldVal.get() ));
				else
					map.put(new Text(curKeySecond), new IntWritable(i.get()));
				
				sum += i.get();
					
			}
			prevKeyFirst = curKeyFirst;
			total += sum;		
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "hybridapproach");
		job.setJarByClass(HybridApproach.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapOutputKeyClass(CustomPair.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}