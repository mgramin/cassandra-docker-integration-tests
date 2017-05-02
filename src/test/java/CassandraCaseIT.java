import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import static org.junit.Assert.assertEquals;

/**
 * Created by maksim on 03.05.17.
 */
public class CassandraCaseIT {

    @Rule
    public GenericContainer cassandra = new GenericContainer("cassandra:latest").withExposedPorts(9042);


    @Test
    public void test() throws InterruptedException {
        Thread.currentThread().sleep(15000);

        Integer mappedPort = cassandra.getMappedPort(9042);
        String containerIpAddress = cassandra.getContainerIpAddress();

        final Cluster cluster = Cluster.builder().withPort(mappedPort).addContactPoint(containerIpAddress).build();
        Session session = cluster.connect("system");

        session.execute("CREATE KEYSPACE test_keyspace WITH replication "
                + "= {'class':'SimpleStrategy', 'replication_factor':1};");

        session = cluster.connect("test_keyspace");

        session.execute("create table test_table(id int primary key, name text)");
        session.execute("insert into test_table(id, name) values(1, 'Jon')");

        ResultSet rows = session.execute("select name from test_table");
        Row one = rows.one();
        String s = one.get(0, String.class);
        assertEquals("Jon", s);
    }

}
