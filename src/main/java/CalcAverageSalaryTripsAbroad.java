import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.ArrayList;

public class CalcAverageSalaryTripsAbroad {
    private static JavaSparkContext sc;

    private CalcAverageSalaryTripsAbroad(JavaSparkContext sc) {
        this.sc = sc;
    }

    private void run() {
        // main circle of program
        // Cassandra -> java
        // java -> spark
        // data from spark java -> cassandra
        CassandraFunctions cassandraFuncs = new CassandraFunctions();
        ArrayList<String> data = cassandraFuncs.getDataFromCassandra();
        JavaPairRDD<String, String> result = SparkCalculations.processData(sc, data);
        cassandraFuncs.writeDataToCassandra(result);
        cassandraFuncs.close();
    }

    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("calcAverage").setMaster("local").set("spark.cassandra.connection.host", "127.0.0.1"));
        CalcAverageSalaryTripsAbroad job = new CalcAverageSalaryTripsAbroad(sc);
        job.run();
        sc.close();
        return;
    }
}