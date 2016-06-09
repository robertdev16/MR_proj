package cs522.proj.MR;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import org.apache.log4j.Logger;

public class Stripes extends Configured implements Tool {

	public static class MapClass extends
			Mapper<Text, Text, StripText, MapWritable> {

		private IntWritable one = new IntWritable(1);

		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {

			if (value != null) {
				String[] listTerm = value.toString().split("\\s+");
				if (listTerm != null) {
					for (int i = 0; i < listTerm.length - 1; i++) {
						String currentTerm = listTerm[i];
						MapWritable stripes = new MapWritable();
						for (int j = i + 1; j < listTerm.length; j++) {
							if (currentTerm.equals(listTerm[j]))
								break;
							StripText curNeighbor = new StripText(listTerm[j]);
							saveDataForStripes(stripes, curNeighbor);
						}

						context.write(new StripText(currentTerm), stripes);

					}
				}
			}

		}

		private void saveDataForStripes(MapWritable stripes, Text curNeighbor) {
			if (stripes.containsKey(curNeighbor)) {
				int counter = ((IntWritable) stripes.get(curNeighbor)).get();
				counter++;
				stripes.put(curNeighbor, new IntWritable(counter));
			} else {
				stripes.put(curNeighbor, one);
			}
		}

	}

	public static class ReduceClass extends
			Reducer<Text, MapWritable, Text, Text> {
		private static final Logger LOG = Logger.getLogger(ReduceClass.class);

		@Override
		public void reduce(Text term, Iterable<MapWritable> stripesList,
				Context context) throws IOException, InterruptedException {
			LOG.debug("Starting reducing");
			SortedMapWritable listTermNeighbor = new SortedMapWritable();
			Iterator<MapWritable> listStripes = stripesList.iterator();
			double stripeTotal = 0.0;
			while (listStripes.hasNext()) {
				MapWritable stripe = listStripes.next();

				stripeTotal = countStripeTotalAndCurVal(listTermNeighbor,
						stripeTotal, stripe);
			}

			for (Entry<WritableComparable, Writable> entry : listTermNeighbor
					.entrySet()) {
				double curVal = ((DoubleWritable) entry.getValue()).get();
				double frequencies = curVal / stripeTotal;

				frequencies = Double.parseDouble(Tools
						.formatDouble(frequencies));
				entry.setValue(new DoubleWritable(frequencies));
			}

			LOG.debug("<Term, listStripes> = (" + term + ", "
					+ Tools.mapWritableToText(listTermNeighbor) + ")");

			context.write(term, Tools.mapWritableToText(listTermNeighbor));

			LOG.debug("Ending reducing");
		}

		private double countStripeTotalAndCurVal(
				SortedMapWritable listTermNeighbor, double stripeTotal,
				MapWritable stripe) {
			for (Entry<Writable, Writable> entry : stripe.entrySet()) {
				Text curNeighbor = (Text) entry.getKey();
				if (listTermNeighbor.containsKey(curNeighbor)) {
					int val1 = ((IntWritable) entry.getValue()).get();
					double val2 = ((DoubleWritable) listTermNeighbor
							.get(curNeighbor)).get();
					stripeTotal += val1;
					double val = val1 + val2;
					listTermNeighbor.put(curNeighbor, new DoubleWritable(val));
				} else {
					int curVal = ((IntWritable) entry.getValue()).get();
					listTermNeighbor.put(curNeighbor,
							new DoubleWritable(curVal));
					stripeTotal += curVal;
				}
			}
			return stripeTotal;
		}
	}

	public static class PartitionerClass extends
			Partitioner<StripText, MapWritable> {
		private static final Logger LOG = Logger
				.getLogger(PartitionerClass.class);

		@Override
		public int getPartition(StripText key, MapWritable value,
				int numPartitions) {
			LOG.debug("Starting getPartition");
			int keyInt = Integer.parseInt(key.toString());

			return keyInt % numPartitions;
		}
	}

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: stripe <input_dir> <output_dir>");
			return 2;
		}

		Path outputDir = new Path(args[1]);
		Configuration conf = getConf();
		final FileSystem fs = FileSystem.get(new URI(args[1]), conf);
		if (fs.exists(outputDir))
			fs.delete(outputDir, true);

		conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
		Job job = Job.getInstance(conf, "Stripes");
		job.setJarByClass(Stripes.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		job.setMapperClass(MapClass.class);
		job.setMapOutputKeyClass(StripText.class);
		job.setMapOutputValueClass(MapWritable.class);
		job.setNumReduceTasks(2);
		job.setPartitionerClass(PartitionerClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(ReduceClass.class);

		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, outputDir);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new Stripes(), args);
		System.exit(ret);
	}
}
