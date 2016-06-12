package cs522.proj.HBase;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
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


public class WeatherStation extends Configured implements Tool {
	public static class MapClass extends Mapper<Text, Text, ImmutableBytesWritable, Put> {
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

		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {

		}
	}

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: weatherstation <input_dir> <table_name>");
			return 2;
		}

		TableName tableName = TableName.valueOf(args[1]);
		Configuration conf = HBaseConfiguration.create();
	    conf.set("hbase.zookeeper.quorum", "Master");
	    HBaseAdmin admin = new HBaseAdmin(conf);
	    if(admin.tableExists(tableName)){
	        System.out.println("table exists!recreating.......");
	        admin.disableTable(tableName);
	        admin.deleteTable(tableName);
	    }

		conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
		Job job = Job.getInstance(conf, "WeatherStation");
		job.setJarByClass(WeatherStation.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		job.setMapperClass(MapClass.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));

	    TableMapReduceUtil.initTableReducerJob(tableName.getNameAsString(), null, job);
	    job.setNumReduceTasks(0);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new WeatherStation(), args);
		System.exit(ret);
	}
}
