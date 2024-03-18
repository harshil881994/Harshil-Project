
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

public class App {
    public static void main(String[] args) {
        // Set up Spark configuration
        SparkConf conf = new SparkConf()
                .setAppName("WordCount")
                .setMaster("local[*]"); // Use local mode for testing, replace with cluster URL in production

        // Create Spark context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load input data
        JavaRDD<String> lines = sc.textFile("input.txt");

        // Perform word count
        
        
        
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        Map<String, Long> wordCounts = words.countByValue();

        // Output word counts
        for (Map.Entry<String, Long> entry : wordCounts.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }

        // Stop Spark context
        sc.stop();
    }
}
