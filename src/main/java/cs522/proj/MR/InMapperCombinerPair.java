package cs522.proj.MR;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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

public class InMapperCombinerPair extends Configured implements Tool {

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
			RFPair pair, star;
			String[] terms = value.toString().split("\\s+");
			for (i = 0; i < terms.length; i++)
				for (j = i + 1; j < terms.length; j++) {
					if (terms[j].equals(terms[i]))
						break;
					pair = new RFPair(terms[i], terms[j]);
					star = new RFPair(terms[i], "*");
					addMapIntValue(H, pair, new IntWritable(1));
					addMapIntValue(H, star, new IntWritable(1));
				}
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			for (Writable pair : H.keySet())
				context.write((RFPair) pair, (IntWritable) H.get(pair));
		}
	}

	public static class ReduceClass extends Reducer<RFPair, IntWritable, Text, Text> {
		private int marginal;
		private Text pairText = new Text();
		private Text rfText = new Text();
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			marginal = 0;
		}

		@Override
		public void reduce(RFPair pair, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			double rfDouble;
			for (IntWritable value : values)
				sum += value.get();
			if ("*".equals(pair.getRight()))
				marginal = sum;
			else if (marginal != 0){
				rfDouble = (double) sum / marginal;
				rfText.set(String.format("%.3f", rfDouble) + " (" + sum + "/" + marginal + ")");
				pairText.set(String.format("%-12s", pair.toString()));
				context.write(pairText, rfText);
			}
		}
	}

	public static class PartitionerClass extends Partitioner<RFPair, IntWritable> {
		@Override
		public int getPartition(RFPair key, IntWritable value, int numPartitions) {
			String left = key.getLeft();
			int result;
			if ((left == null) || (left.isEmpty()))
				result = 0;
			try {
				result = Integer.parseInt(left);
			} catch (NumberFormatException nfe) {
				result = 0;
			}

			return result % numPartitions;
		}
	}

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: pair <input_dir> <output_dir>");
			return 2;
		}

		Path outputDir = new Path(args[1]);
		Configuration conf = getConf();
		final FileSystem fs = FileSystem.get(new URI(args[1]), conf);
		if (fs.exists(outputDir))
			fs.delete(outputDir, true);

		conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
		Job job = Job.getInstance(conf, "InMapperCombinerPair");
		job.setJarByClass(InMapperCombinerPair.class);
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
		int ret = ToolRunner.run(new InMapperCombinerPair(), args);
		System.exit(ret);
	}

}
