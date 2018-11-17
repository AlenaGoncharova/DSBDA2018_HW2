import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.ArrayList;

public class CassandraFunctions {

    private static Cluster cluster;
    private static Session session;

    public ArrayList<String> getDataFromCassandra() {
        // initialize casandra connection
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect("citizens_statistics");

        // create sql query and get draft data
        Select query = QueryBuilder
                .select().from("citizens_statistics","data");
        ResultSet rs = session.execute(query.toString());
        ArrayList<String> data = new ArrayList<>();

        // convert data to str with separator
        for (Row row : rs) {
            data.add(row.getString("passportNumber") + "\t".concat(String.valueOf(row.getInt("month"))) +
                    "\t".concat(String.valueOf(row.getInt("data"))));
            System.out.println(row.getString("passportNumber"));
            System.out.println("data" + row.getInt("data"));
            System.out.println("month" + row.getInt("month"));
        }
        return data;
    }

    public void writeDataToCassandra(JavaPairRDD<String, String> data) {
        // write processed data to cassandra
        data.collectAsMap().forEach((k,v) -> {
            String[] calculatedData = v.split(",");
            double s = Double.parseDouble(calculatedData[0]);
            double t = Double.parseDouble(calculatedData[1]);
            Insert insert = QueryBuilder
                    .insertInto("citizens_statistics", "citizens")
                    .value("passportNumber", k)
                    .value("mean_salary", s)
                    .value("mean_trips", t)

                    .value("id", k);
            session.execute(insert.toString());
        });
    }

    public void close() {
        // close cassandra connection
        session.close();
        cluster.close();
    }
}
