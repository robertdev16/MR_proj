package cs522.proj.Spark;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SameFirstName {

//	@SuppressWarnings("serial")
//	private static final PairFunction<Smartphone, String, Integer> SMARTPHONE_MAPPER = new PairFunction<Smartphone, String, Integer>() {
//		public Tuple2<String, Integer> call(Smartphone smartphone)
//				throws Exception {
//			return new Tuple2<String, Integer>(smartphone.getBrand(),
//					smartphone.getPcs());
//		}
//
//	};

	@SuppressWarnings("serial")
	private static final Function2<Integer, Integer, Integer> SMARTPHONE_REDUCER = new Function2<Integer, Integer, Integer>() {

		public Integer call(Integer pcs1, Integer pcs2) throws Exception {
			return pcs1 + pcs2;
		}

	};

	@SuppressWarnings("serial")
//	private static final FlatMapFunction<String, Smartphone> SMARTPHONE_EXTRACTOR = new FlatMapFunction<String, Smartphone>() {
//		public Iterable<Smartphone> call(String s) throws Exception {
//			String[] lineText = s.split("\n");
//			List<Smartphone> list = new ArrayList<Smartphone>();
//			if (lineText != null) {
//				for (int i = 0; i < lineText.length; i++) {
//					String line = lineText[i];
//					String[] smartphoneData = line.split(" ");
//					String brand = smartphoneData[0];
//					int pcs = Integer.parseInt(smartphoneData[1]);
//					Smartphone smartphone = new Smartphone(brand, pcs);
//					list.add(smartphone);
//
//				}
//			}
//			return list;
//		}
//	};

	public static void main(String[] args) throws IOException, URISyntaxException {
		if (args.length != 2) {
			System.err.println("usage <input_dir> <output_dir>");
			System.exit(0);
		}
		
		Path outputDir = new Path(args[1]);
		Configuration conf2 = new Configuration();
		final FileSystem fs = FileSystem.get(new URI(args[1]), conf2);
		if (fs.exists(outputDir))
			fs.delete(outputDir, true);

		SparkConf conf = new SparkConf().setAppName(
				"edu.mum.smartphonesales.Sales").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> file = context.textFile(args[0]);
		
//		JavaRDD<Smartphone> SMARTPHONE = file.flatMap(SMARTPHONE_EXTRACTOR);
//		JavaPairRDD<String, Integer> pairs = SMARTPHONE.mapToPair(SMARTPHONE_MAPPER);
//		JavaPairRDD<String, Integer> counter = pairs.reduceByKey(SMARTPHONE_REDUCER);
//		
//
//		counter.saveAsTextFile(args[1]);
	}

}