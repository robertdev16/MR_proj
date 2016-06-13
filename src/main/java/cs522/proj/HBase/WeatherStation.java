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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class WeatherStation extends Configured implements Tool {
	private static final Logger logger = Logger.getLogger(WeatherStation.class);
	private static final String columnFamily = "info";
	private static final String[] qualifiers = {"lat", "lng", "elev", "state", "name"};

	public WeatherStation() {
		super();
	}

	public WeatherStation(Configuration conf) {
		super(conf);
	}

	public static class MapClass extends
			Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
		private long checkpoint = 100;
		private long count = 0;
		private Put put;
		private String[] cellStrs = new String[5];

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String valueStr = value.toString();
			String rowKey = valueStr.substring(0, 11).trim();
			cellStrs[0] = valueStr.substring(12, 20).trim();
			cellStrs[1] = valueStr.substring(21, 30).trim();
			cellStrs[2] = valueStr.substring(31, 37).trim();
			cellStrs[3] = valueStr.substring(38, 40).trim();
			cellStrs[4] = valueStr.substring(41, 71).trim();
			put = new Put(Bytes.toBytes(rowKey));
			for (int i=0; i<5; i++){
				if (! cellStrs[i].isEmpty())
					put.addColumn(Bytes.toBytes(columnFamily),
							Bytes.toBytes(qualifiers[i]), Bytes.toBytes(cellStrs[i]));
			}
			context.write(new ImmutableBytesWritable(Bytes.toBytes(rowKey)), put);
						
			if(++count % checkpoint == 0)
				context.setStatus("Emitting Put " + count);
		}
	}

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: weatherstation <input_dir> <table_name>");
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

		Job job = Job.getInstance(conf, "WeatherStation");
		job.setJarByClass(WeatherStation.class);

		job.setMapperClass(MapClass.class);
		TableMapReduceUtil.initTableReducerJob(tableName.getNameAsString(),	null, job);
		job.setNumReduceTasks(0);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(
				new WeatherStation(HBaseConfiguration.create()), args);
		System.exit(ret);
	}
}
