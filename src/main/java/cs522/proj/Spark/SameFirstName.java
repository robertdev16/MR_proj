package cs522.proj.Spark;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SameFirstName {

	public static void main(String[] args) throws IOException,
			URISyntaxException {
		if (args.length != 2) {
			System.err.println("usage <input_dir> <output_dir>");
			System.exit(0);
		}

		Path outputDir = new Path(args[1]);
		final FileSystem fs = FileSystem.get(new URI(args[1]),
				new Configuration());
		if (fs.exists(outputDir))
			fs.delete(outputDir, true);

		JavaSparkContext jsc = new JavaSparkContext(
				new SparkConf().setAppName("SameFirstName").setMaster("local"));

		JavaRDD<String> lines = jsc.textFile(args[0]);

		JavaPairRDD<String, TreeMap<Integer, Integer>> pairs = lines.mapToPair(
				new PairFunction<String, String, TreeMap<Integer, Integer>>() {
					private static final long serialVersionUID = -883167566644322156L;

					@Override
					public Tuple2<String, TreeMap<Integer, Integer>> call(
							String lineStr) {
						TreeMap<Integer, Integer> H = new TreeMap<>();
						String[] keyValue = lineStr.split("\t");
						String firstName = keyValue[0].split(" ")[0];
						String[] items = keyValue[1].split(" ");
						Integer intId;
						for (String productId : items) {
							try {
								intId = new Integer(productId);
							} catch (NumberFormatException nfe) {
								continue;
							}
							if (H.containsKey(intId)) {
								H.put(intId, new Integer(H.get(intId) + 1));
							} else
								H.put(intId, Integer.valueOf(1));
						}
						H.put(Integer.MIN_VALUE, Integer.MIN_VALUE);	//Assistant entry help to tell whether reduced already when output 
						return new Tuple2<String, TreeMap<Integer, Integer>>(
								firstName, H);
					}
				}).reduceByKey(
				new Function2<TreeMap<Integer, Integer>, TreeMap<Integer, Integer>, TreeMap<Integer, Integer>>() {
					private static final long serialVersionUID = -8764105069259039613L;

					@Override
					public TreeMap<Integer, Integer> call(
							TreeMap<Integer, Integer> t1,
							TreeMap<Integer, Integer> t2) {
						
						if (t1.isEmpty()) return t1;
						if (t2.isEmpty()) return t2;
						HashSet<Integer> hSet = new HashSet<>();
						for (Integer key : t1.keySet()){
							if (t2.containsKey(key))
								t1.put(key, new Integer(t1.get(key) + t2.get(key)));
							else
								hSet.add(key);
						}
						for (Integer key : hSet)
							t1.remove(key);

						t1.remove(Integer.MIN_VALUE);		//Remove assistant entry if reduced already
						return t1;
					}
				}).filter(
				new Function<Tuple2<String, TreeMap<Integer, Integer>>, Boolean>() {
					private static final long serialVersionUID = 1163555580389979962L;

					@Override
					public Boolean call(Tuple2<String, TreeMap<Integer, Integer>> tuple) {
						return (! tuple._2().containsKey(Integer.MIN_VALUE));
					}
				});

		pairs.sortByKey().saveAsTextFile(args[1]);
		jsc.close();
	}
}