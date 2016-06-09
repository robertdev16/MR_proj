package cs522.proj.MR;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Hybrid extends Configured implements Tool {

	public static class MapClass extends Mapper<Text, Text, RFPair, IntWritable> {
		private MapWritable H;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			H = new MapWritable();
		}

		private void addMapIntValue(MapWritable map, Writable key, IntWritable newValue) {
			if (map.containsKey(key)) {
				IntWritable oldValue = (IntWritable) map.get(key);
				map.put(key, new IntWritable(oldValue.get() + newValue.get()));
			} else
				map.put(key, newValue);
		}

		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			int i, j;
			RFPair pair;
			String[] terms = value.toString().split("\\s+");
			for (i = 0; i < terms.length; i++)
				for (j = i + 1; j < terms.length; j++) {
					if (terms[j].equals(terms[i]))
						break;
					pair = new RFPair(terms[i], terms[j]);
					addMapIntValue(H, pair, new IntWritable(1));
				}
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			for (Writable pair : H.keySet())
				context.write((RFPair) pair, (IntWritable) H.get(pair));
		}
	}

	public static class ReduceClass extends Reducer<RFPair, IntWritable, Text, Text> {
		private SortedMapWritable sortedH;
		private int marginal;
		private String currentLeft;
		private Text pairText = new Text();
		private Text rfText = new Text();
		private RFPair emitPair = new RFPair();
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			sortedH = new SortedMapWritable();
			marginal = 0;
			currentLeft = "*";
		}

		@Override
		public void reduce(RFPair pair, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			String left = pair.getLeft();
			String right = pair.getRight();
			int sum = 0;
			
			for (IntWritable value : values)
				sum += value.get();
			if (left.equals(currentLeft))
				marginal += sum;
			else{
				emitRF(context);
				currentLeft = new String(left);
				marginal = sum;
				sortedH.clear();
			}
			sortedH.put(new IntWritable(convertInt(right)), new IntWritable(sum));
		}
		
		@SuppressWarnings("rawtypes")
		private void emitRF(Context context) throws IOException, InterruptedException {
			double rfDouble;
			int rightCount;
			for (WritableComparable rightInt : sortedH.keySet()){
				rightCount = ((IntWritable) sortedH.get(rightInt)).get();
				rfDouble = (double) rightCount / marginal;
				emitPair.set(currentLeft, Integer.toString(((IntWritable) rightInt).get()));
				rfText.set(String.format("%.3f", rfDouble) + " (" + rightCount + "/" + marginal + ")");
				pairText.set(String.format("%-12s", emitPair.toString()));
				context.write(pairText, rfText);
			}
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			emitRF(context);
		}
	}

	public static int convertInt(String s) {
		if ((s == null) || (s.isEmpty()))
			return 0;
		int result;
		try {
			result = Integer.parseInt(s);
		} catch (NumberFormatException nfe) {
			result = 0;
		}
		return result;
	}
	
	public static class PartitionerClass extends Partitioner<RFPair, IntWritable> {
		@Override
		public int getPartition(RFPair key, IntWritable value, int numPartitions) {
			String left = key.getLeft();
			return convertInt(left) % numPartitions;
		}
	}

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: hybrid <input_dir> <output_dir>");
			return 2;
		}

		Path outputDir = new Path(args[1]);
		Configuration conf = getConf();
		final FileSystem fs = FileSystem.get(new URI(args[1]), conf);
		if (fs.exists(outputDir))
			fs.delete(outputDir, true);

		conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
		Job job = Job.getInstance(conf, "Hybrid");
		job.setJarByClass(Hybrid.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		job.setMapperClass(MapClass.class);
		job.setMapOutputKeyClass(RFPair.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setNumReduceTasks(2);
		job.setPartitionerClass(PartitionerClass.class);

		job.setReducerClass(ReduceClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, outputDir);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new Hybrid(), args);
		System.exit(ret);
	}

}
