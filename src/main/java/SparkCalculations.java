import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;
import scala.Tuple4;

import java.util.ArrayList;

public class SparkCalculations {

    /**
     * Class which process data from casandra and apply spark job.
     * @param sc
     * @param dataCitizens
     * @return
     */
    public static JavaPairRDD<String, String> processData(JavaSparkContext sc, ArrayList<String> dataCitizens) {

        JavaRDD<String> dataCitizensRDD = sc.parallelize(dataCitizens);

        JavaPairRDD<String, String> result = dataCitizensRDD
                .mapToPair((st) -> {
                String[] record = st.split("\t") ;
                return SparkCalculations.identValue(record);
        })
            .reduceByKey((Function2<Tuple4<Integer, Integer, Integer, Integer>, Tuple4<Integer, Integer, Integer, Integer>, Tuple4<Integer, Integer, Integer, Integer>>)
                    (x1, x2) -> new Tuple4<>(x1._1() + x2._1(), x1._2() + x2._2(), x1._3() + x2._3(), x1._4() + x2._4()))
            .mapValues((Function<Tuple4<Integer, Integer, Integer, Integer>, String>)
                f -> {
                    return SparkCalculations.calcMeanAmount(f);
                }
            );
        return result;
    }

    /**
     * A method which identify value as salary or as number of trips.
     * Returns <salary, 1, 0, 0> if the value is defined as salary,
     * and <0, 0, the count of trips, 1> if he value is defined as the number of trips.
     * @param record
     * @return
     */
    public static Tuple2<String,Tuple4<Integer,Integer,Integer,Integer>> identValue(String[] record) {
        String passportNumber = record[0];
        int salary = 0;
        int trips = 0;
        int current = Integer.parseInt(record[2]);

        if ( current > 30) {
            salary = current;
            return new Tuple2<>(passportNumber, new Tuple4<>(salary, 1, trips, 0));
        }
        else {
            trips = current;
            return new Tuple2<>(passportNumber, new Tuple4<>(salary, 0, trips, 1));
        }
    }

    /**
     * A method that calculates the average salary and the average count of trips.
     * @param x
     * @return
     */
    public static String calcMeanAmount(Tuple4<Integer, Integer, Integer, Integer> x) {
        Integer sumSalary = x._1();
        Integer countSalary = x._2();
        Integer sumTrips = x._3();
        Integer countTrips= x._4();
        double meanSalary = 0.0;
        if (countSalary != 0) {
            meanSalary = (double) sumSalary / countSalary;
        }
        double meanTrips = 0.0;
        if (countTrips != 0) {
            meanTrips = (double) sumTrips / countTrips;
        }
        return String.valueOf(meanSalary) + "," + String.valueOf(meanTrips);
    }


}
