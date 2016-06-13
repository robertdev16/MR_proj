package cs522.proj.HBase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import cs522.proj.MR.Stripes.ReduceClass;


public class WeatherStation extends Configured implements Tool {
	private static final Logger logger = Logger.getLogger(WeatherStation.class);

	public WeatherStation() {
		super();
	}

	public WeatherStation(Configuration conf) {
		super(conf);
	}

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
		
		final String columnFamily = "s";
		final String[] qualifiers = {"Lat", "Lng", "ASL", "Name"};
		
		
		TableName tableName = TableName.valueOf(args[1]);
		Configuration conf = getConf();
		
		try (Connection connection = ConnectionFactory.createConnection(conf)){
			Admin admin = connection.getAdmin();
		    if(admin.tableExists(tableName)){
		        logger.info("Table " + args[1] + " exists! Recreating...");
		        admin.disableTable(tableName);
		        admin.deleteTable(tableName);
		    }
		    
		    HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
		    hTableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
		    admin.createTable(hTableDescriptor);
		    
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
		int ret = ToolRunner.run(new WeatherStation(HBaseConfiguration.create()), args);
		System.exit(ret);
	}
}

/**
 * Sample data:
 * CH000054511  39.9330  116.2830   55.0    BEIJING                        GSN     54511
 * SF001760150 -30.7500   27.0200 1497.0    PAARDEFONTEIN                               
 * USC00132789  41.0211  -91.9553  225.6 IA FAIRFIELD                          HCN      
 * US10red_020  40.2973 -100.2131  737.9 NE CAMBRIDGE 2.7 WNW                           
 */
