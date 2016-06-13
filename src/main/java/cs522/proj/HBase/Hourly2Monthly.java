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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class Hourly2Monthly extends Configured implements Tool {
	private static final Logger logger = Logger.getLogger(Hourly2Monthly.class);
	private static final String columnFamily = "tavg";

	public Hourly2Monthly() {
		super();
	}

	public Hourly2Monthly(Configuration conf) {
		super(conf);
	}

	public static class MapClass extends
			Mapper<LongWritable, Text, Text, FloatWritable> {
		private Text mapOutKey = new Text();
		private FloatWritable mapOutValue = new FloatWritable();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String valueStr = value.toString();
			String stationWithMonth = valueStr.substring(0, 11).trim() + "-" + valueStr.substring(12, 14);
			
			int count = 0;
			int sum = 0;
			int i, pos, FahrInt;
			String FahrStr;
			for (i=0; i<24; i++) {
				pos = 18 + 7 * i;
				FahrStr = valueStr.substring(pos, pos + 5);
				if ((FahrStr.trim().isEmpty()) || (FahrStr.charAt(0) == '-'))
					continue;
				try {
					FahrInt = Integer.parseInt(FahrStr.trim());
				}
				catch (NumberFormatException nfe) {
					continue;
				}
				count++;
				sum += FahrInt;
			}
			if (count == 0){
				logger.warn("In mapper count == 0, stationWithMonth is " + stationWithMonth);
				return;
			}

			mapOutKey.set(stationWithMonth);
			mapOutValue.set((sum / count) / 10);
			context.write(mapOutKey, mapOutValue);
		}
	}
	
	public static class ReducerClass extends TableReducer<Text, FloatWritable, ImmutableBytesWritable>  {
		private Put put;
		
		@Override
		public void reduce(Text key, Iterable<FloatWritable> values, Context context)
				throws IOException, InterruptedException {
			
			String keyStr = key.toString();
			String rowKey = keyStr.substring(0, 11);
			String monthStr = keyStr.substring(12, 14);
			put = new Put(Bytes.toBytes(rowKey));
			
			int count = 0;
			float sum = 0.0f;
			for (FloatWritable value : values){
				count++;
				sum += value.get();
			}
			if (count == 0){
				logger.warn("In reducer count == 0, rowKey is " + rowKey);
				return;
			}

			String cellStr = String.format("%.2f", sum / count).trim();
			put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(monthStr), Bytes.toBytes(cellStr));
			context.write(new ImmutableBytesWritable(Bytes.toBytes(rowKey)), put);
		}

	}

	public static class PartitionerClass extends Partitioner<Text, FloatWritable> {
		@Override
		public int getPartition(Text key, FloatWritable value, int numPartitions) {
			String keyStr = key.toString();
			return keyStr.charAt(10) % numPartitions;
		}
	}
	
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: hourly2monthly <input_dir> <table_name>");
			return 2;
		}

		TableName tableName = TableName.valueOf(args[1]);
		Configuration conf = getConf();

		try (Connection connection = ConnectionFactory.createConnection(conf)) {
			Admin admin = connection.getAdmin();
			if (admin.tableExists(tableName)) {
				logger.info("Table " + args[1] + " exists! Recreating...");
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
			}

			HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
			hTableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
			admin.createTable(hTableDescriptor);
		}

		Job job = Job.getInstance(conf, "Hourly2Monthly");
		job.setJarByClass(Hourly2Monthly.class);

		job.setMapperClass(MapClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		TableMapReduceUtil.initTableReducerJob(tableName.getNameAsString(),
				ReducerClass.class, job, PartitionerClass.class);
		job.setNumReduceTasks(2);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(
				new Hourly2Monthly(HBaseConfiguration.create()), args);
		System.exit(ret);
	}
}
