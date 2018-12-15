import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import scala.Tuple2;
import scala.Tuple4;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

/**
 * SparkCalculationsTest.java
 * Unit tests for SparkCalculations class.
 */
public class SparkCalculationsTest {

    /**
     * Correct test identification of the value - salary or trips, and forming a structure for further aggregation.
     */
    @Test
    public void identValueTest() {
        String[] record = ("34 587842" + "\t" +  2 + "\t" + "6").split("\t") ;
        Tuple2<String, Tuple4<Integer,Integer,Integer,Integer>> result = new Tuple2<>("34 587842",new Tuple4<>(0,0,6,1));
        assertEquals(result, SparkCalculations.identValue(record));
    }

    /**
     * Correct test of calculating average salary and average count trips.
     */
    @Test
    public void calcMeanAmountTest() {
        Tuple4<Integer, Integer, Integer, Integer> x = new Tuple4<>(300000, 3, 12, 4);
        assertEquals("100000.0,3.0",SparkCalculations.calcMeanAmount(x));
    }

    /**
     * Test to verify that the record was processed.
     */
    @Test
    public void processDataTest() {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("calcAverage").setMaster("local").set("spark.cassandra.connection.host", "127.0.0.1").set("spark.driver.allowMultipleContexts", "true"));
        ArrayList<String> dataForProcess = new ArrayList<>();
        dataForProcess.add("91 05 634485" + "\t".concat("4") + "\t".concat(String.valueOf("67800")));

        ArrayList<Tuple2<String, String>> canonResultData = new ArrayList<>();
        canonResultData.add(new Tuple2<>("91 05 634485", "67800,0"));

        JavaPairRDD<String, String> resultRDD = SparkCalculations.processData(sc, dataForProcess);
        ArrayList<Tuple2<String, String>> resultData = new ArrayList<>();

        resultRDD.foreach(x -> {
            resultData.add(new Tuple2<>(x._1 ,x._2));
        });

        assert(resultRDD.count() == 1);
    }


}
