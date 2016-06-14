package cs522.proj.HBase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class YearlyGoldenRatio extends Configured implements Tool {
	private static final Logger logger = Logger.getLogger(YearlyGoldenRatio.class);
	private static final String columnFamily = "tavg";
	private static final String[] qualifiers = {"yearly", "diffGR"};
	private static final float tBodyInCelsius = 37.0f;
	private static final float goldenRatio = 0.618f;
	private static final float tGRInFahrenheit = (tBodyInCelsius * goldenRatio) * 1.8f + 32.0f;

	public YearlyGoldenRatio() {
		super();
	}

	public YearlyGoldenRatio(Configuration conf) {
		super(conf);
	}

	public static class MapClass extends TableMapper<ImmutableBytesWritable, Put> {
		private Put put;
		private Cell[] cells;

		@Override
		public void map(ImmutableBytesWritable rowKey, Result value, Context context)
				throws IOException, InterruptedException {
			
			if (value.isEmpty())
				return;
			cells = value.rawCells();
			float monthlyFloat;
			int count = 0;
			float yearlySum = 0;
			float diffGRSum = 0;
			for (int i=0; i<cells.length; i++){
				try{
					monthlyFloat = Float.parseFloat(new String(CellUtil.cloneValue(cells[i])));
				}
				catch (NumberFormatException nfe) {
					continue;
				}
				count++;
				yearlySum += monthlyFloat;
				diffGRSum += Math.abs(monthlyFloat - tGRInFahrenheit);				
			}

			if (count == 0){
				logger.warn("In mapper count == 0, rowKey is " + new String(rowKey.get()));
				return;
			}

			put = new Put(rowKey.get());
			put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifiers[0]),
					Bytes.toBytes(String.format("%.4f", yearlySum / count).trim()));
			put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifiers[1]),
					Bytes.toBytes(String.format("%.4f", diffGRSum / count).trim()));
			context.write(rowKey, put);
		}
	}
	
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: yearlygoldenratio <input_table> <output_table>");
			return 2;
		}

		TableName sourceTable = TableName.valueOf(args[0]);
		TableName destTable = TableName.valueOf(args[1]);
		Configuration conf = getConf();

		try (Connection connection = ConnectionFactory.createConnection(conf)) {
			Admin admin = connection.getAdmin();
			if (admin.tableExists(destTable)) {
				logger.info("destTable " + args[1] + " exists! Recreating...");
				admin.disableTable(destTable);
				admin.deleteTable(destTable);
			}

			HTableDescriptor hTableDescriptor = new HTableDescriptor(destTable);
			hTableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
			admin.createTable(hTableDescriptor);
		}

		Job job = Job.getInstance(conf, "YearlyGoldenRatio");
		job.setJarByClass(YearlyGoldenRatio.class);

		Scan scan = new Scan();
		scan.setCaching(500);
		scan.setCacheBlocks(false);
		TableMapReduceUtil.initTableMapperJob(sourceTable, scan, MapClass.class,
				ImmutableBytesWritable.class, Put.class, job);
		
		TableMapReduceUtil.initTableReducerJob(destTable.getNameAsString(),	null, job);
		job.setNumReduceTasks(0);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(
				new YearlyGoldenRatio(HBaseConfiguration.create()), args);
		System.exit(ret);
	}
}
