import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * CassandraFunctionsTest.java
 * Tests for CassandraFunctions class.
 */
public class CassandraFunctionsTest {

    private JavaSparkContext sc;
    private CassandraFunctions cassFunc;

    /**
     * Creates SparkContext and CassandraFunctions before each try.
     */
    @Before
    public void setup() {
        sc = new JavaSparkContext(new SparkConf().setAppName("calcAverage").setMaster("local").set("spark.cassandra.connection.host", "127.0.0.1").set("spark.driver.allowMultipleContexts", "true"));
        cassFunc = new CassandraFunctions();
    }

    /**
     * Correct test receiving data from the empty table in Сassandra.
     */
    @Test
    public void getDataFromEmptyCassandraTest() {
        cassFunc.deleteDataFromTable("citizens_statistics", "data");
        assertEquals(0, cassFunc.getDataFromCassandra().size());
    }

    /**
     * Correct test receiving data from the non-empty table in Сassandra.
     */
    @Test
    public void getDataFromNotEmptyCassandraTest() {
        cassFunc.getSession().execute("INSERT INTO citizens_statistics.data (id,passportNumber, month, data) VALUES (0, '91 05 634485', 4, 952855)");

        assertNotEquals(0, cassFunc.getDataFromCassandra().size());
    }

    /**
     * Test for getting correct data from the table in cassandra.
     */
    @Test
    public void getSpecificDataFromNotEmptyCassandraTest() {
        cassFunc.deleteDataFromTable("citizens_statistics", "data");
        cassFunc.getSession().execute("INSERT INTO citizens_statistics.data (id,passportNumber, month, data) VALUES (0, '91 05 634485', 4, 952855)");
        ArrayList<String> data = new ArrayList<>();
        data.add("91 05 634485" + "\t".concat("4") + "\t".concat("952855"));

        assertEquals(data, cassFunc.getDataFromCassandra());
    }

    /**
     * Correct test for recording of data to Cassandra.
     */
    @Test
    public void writeDataToCassandraTest() {
        cassFunc.deleteDataFromTable("citizens_statistics", "citizens");

        ArrayList<Tuple2<String, String>> dataForWritten = new ArrayList<>();
        dataForWritten.add( new Tuple2<>("6010 523567", "25678,7"));
        dataForWritten.add( new Tuple2<>("4515 765468", "78546,3"));
        JavaPairRDD<String, String> dataForWrittenRDD = sc.parallelizePairs(dataForWritten);

        cassFunc.writeDataToCassandra(dataForWrittenRDD);

        Select query = QueryBuilder
                .select().from("citizens_statistics","citizens");
        ResultSet rs = cassFunc.getSession().execute(query.toString());
        ArrayList<Tuple2<String, String>> dataFromCas = new ArrayList<>();

        // convert data to str with separator
        for (Row row : rs) {
            dataFromCas.add(new Tuple2 <>(row.getString("passportNumber"), (String.valueOf(row.getInt("mean_salary")) +
                    ",".concat(String.valueOf(row.getInt("mean_trips"))))));
        }

        Set< Tuple2<String, String>> listWriten = new HashSet<>(dataForWritten);
        Set< Tuple2<String, String>> listRead = new HashSet<>(dataFromCas);
        assert(listWriten.equals(listRead));

    }
}
