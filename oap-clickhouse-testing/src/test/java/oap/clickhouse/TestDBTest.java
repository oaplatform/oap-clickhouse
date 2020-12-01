/*
 * The MIT License (MIT)
 *
 * Copyright (c) Open Application Platform Authors
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

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
    private static final String HOST = Env.getEnvOrDefault( "CLICKHOUSE_HOST", "localhost" );

    @Test
    public void test() throws IOException {
        var counter = new MutableInt();
        var count = 1;

        var clickHouseClient = new DefaultClickHouseClient( HOST, 8123, TestDB.testDbName( "db" ), 1048576, 1048576 );
        clickHouseClient.createDatabase();
        try {
            clickHouseClient.execute( "CREATE TABLE TEST (A INTEGER, B String, C String) ENGINE = MergeTree ORDER BY (A)", true );
            try( var out = clickHouseClient.put( "TEST", DataFormat.TabSeparated ) ) {
                try {
                    for( var i = 0; i < count; i++ ) {
                        out.write( ( "" + i + "\tskjdfhskdjfdsklfjkdsfh sdkjhg sdkfhg dsjkhg sdkfjg hdskfjgh sdkjgh d\t\n" ).getBytes() );
                    }
                } catch( IOException e ) {
                    throw new UncheckedIOException( e );
                }
            }


            clickHouseClient.get( "SELECT * FROM TEST", str -> counter.increment() );
        } finally {
            TestDB.dropDatabases( clickHouseClient );
        }

        assertThat( counter.getValue() ).isEqualTo( count );
    }
}
