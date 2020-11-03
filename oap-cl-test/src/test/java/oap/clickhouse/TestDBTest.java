package oap.clickhouse;

import oap.testng.Env;
import org.apache.commons.lang3.mutable.MutableInt;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by igor.petrenko on 02.03.2018.
 */
public class TestDBTest {
    private static final String HOST = Env.getEnvOrDefault("CLICKHOUSE_HOST", "localhost");

    @Test
    public void test() throws IOException {
        var counter = new MutableInt();
        var count = 1;

        var clickHouseClient = new DefaultClickHouseClient(HOST, 8123, TestDB.testDbName("db"), 1048576, 1048576);
        clickHouseClient.createDatabase();
        try {
            clickHouseClient.execute("CREATE TABLE TEST (A INTEGER, B String, C String) ENGINE = MergeTree ORDER BY (A)", true);
            try (var out = clickHouseClient.put("TEST", DataFormat.TabSeparated)) {
                try {
                    for (var i = 0; i < count; i++) {
                        out.write(("" + i + "\tskjdfhskdjfdsklfjkdsfh sdkjhg sdkfhg dsjkhg sdkfjg hdskfjgh sdkjgh d\t\n").getBytes());
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }


            clickHouseClient.get("SELECT * FROM TEST", str -> counter.increment());
        } finally {
            TestDB.dropDatabases(clickHouseClient);
        }

        assertThat(counter.getValue()).isEqualTo(count);
    }
}
