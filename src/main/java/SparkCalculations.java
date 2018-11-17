import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;
import scala.Tuple4;

import java.util.ArrayList;

public class SparkCalculations {

    public static JavaPairRDD<String, String> processData(JavaSparkContext sc, ArrayList<String> dataCitizens) {
        // Process data from casandra and apply spark job

        JavaRDD<String> dataCitizensRDD = sc.parallelize(dataCitizens);
//        citizensRDD.saveAsTextFile("ouput1");
        JavaPairRDD<String, String> result = dataCitizensRDD
                .mapToPair((st) -> {
                String[] record = st.split("\t") ;

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
        })
            .reduceByKey((Function2<Tuple4<Integer, Integer, Integer, Integer>, Tuple4<Integer, Integer, Integer, Integer>, Tuple4<Integer, Integer, Integer, Integer>>)
                    (x1, x2) -> new Tuple4<>(x1._1() + x2._1(), x1._2() + x2._2(), x1._3() + x2._3(), x1._4() + x2._4()))
            .mapValues((Function<Tuple4<Integer, Integer, Integer, Integer>, String>)
                f -> {
                    Integer sumSalary = f._1();
                    Integer countSalary = f._2();
                    Integer sumTrips = f._3();
                    Integer countTrips= f._4();
                    double meanSalary = 0.0;
                    if (countSalary != 0) {
                        meanSalary = (double)sumSalary/countSalary;
                    }
                    double meanTrips = 0.0;
                    if (countTrips != 0) {
                        meanTrips = (double)sumTrips/countTrips;
                    }
                    return String.valueOf(meanSalary) + "," + String.valueOf(meanTrips);
                }
            );
//        data.saveAsTextFile("ouput2");
        return result;
    }


}
