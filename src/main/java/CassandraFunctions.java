import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.ArrayList;

/**
 * Class that contains methods for working with Cassandra
 */
public class CassandraFunctions {

    private static Cluster cluster;
    private static Session session;

    /**
     * Initialize casandra connection
     */
    public CassandraFunctions() {
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect("citizens_statistics");
    }

    /**
     * The method for getting session.
     * @return
     */
    public Session getSession() {
        return session;
    }

    /**
     * The method that get data from table "data" of Cassandra.
     * @return
     */
    public ArrayList<String> getDataFromCassandra() {

        // create sql query and get draft data
        Select query = QueryBuilder
                .select().from("citizens_statistics","data");
        ResultSet rs = session.execute(query.toString());
        ArrayList<String> data = new ArrayList<>();

        // convert data to str with separator
        for (Row row : rs) {
            data.add(row.getString("passportNumber") + "\t".concat(String.valueOf(row.getInt("month"))) +
                    "\t".concat(String.valueOf(row.getInt("data"))));
        }
        return data;
    }

    /**
     * The method that write processed data to table "citizens" of Cassandra.
     * @param data
     */
    public void writeDataToCassandra(JavaPairRDD<String, String> data) {
        data.foreach(x -> {
            String[] calculatedData = x._2.split(",");
            double s = Double.parseDouble(calculatedData[0]);
            double t = Double.parseDouble(calculatedData[1]);

            Insert insert = QueryBuilder
                    .insertInto("citizens_statistics", "citizens")
                    .value("passportNumber", x._1)
                    .value("mean_salary", (int) Math.round(s))
                    .value("mean_trips", (int) Math.round(t))
                    .value("id", x._1);
            session.execute(insert.toString());
        });
    }

    /**
     * The method that all data from some table.
     * @param keyspace
     * @param table
     */
    public void deleteDataFromTable(String keyspace, String table) {
        session.execute("TRUNCATE " + keyspace + "." + table + ";");
    }

    /**
     * The method for closing session.
     */
    public void close() {
        // close cassandra connection
        session.close();
        cluster.close();
    }
}