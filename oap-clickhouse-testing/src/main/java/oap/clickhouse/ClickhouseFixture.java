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

import oap.system.Env;
import oap.testng.EnvFixture;
import oap.testng.Teamcity;
import oap.util.Dates;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;

import java.util.List;

import static oap.testng.Fixture.Scope.METHOD;

/**
 * ENV:
 * - CLICKHOUSE_HOST
 * - DATABASE_NAME
 */
public class ClickhouseFixture extends EnvFixture<ClickhouseFixture> {
    public static final String CLICKHOUSE_HOST = "CLICKHOUSE_HOST";
    public static final String DATABASE_NAME = "DATABASE_NAME";

    private final String clickhouseHost = Env.get( CLICKHOUSE_HOST, "localhost" );
    private final String databaseName;
    private final String testDatabaseName;

    public ClickhouseFixture( String databaseName ) {
        this( METHOD, databaseName );
    }

    public ClickhouseFixture( Scope scope, String databaseName ) {
        this.scope = scope;
        this.databaseName = databaseName;
        testDatabaseName = testDbName( this.databaseName );
        define( DATABASE_NAME, testDatabaseName );
        define( CLICKHOUSE_HOST, clickhouseHost );
    }

    public static String testDbName( String database ) {
        return StringUtils.replaceChars( database + "_" + Teamcity.buildPrefix(), ".-", "_" );
    }

    public static void dropDatabases( ClickhouseClient client ) {
        var time = DateTime.now().minusDays( 2 ).toString( "YYYY-MM-dd HH:mm:ss" );
        List<String> lines = client
            .useDatabase( "system" )
            .getLines( "SELECT database FROM (SELECT database, MAX(metadata_modification_time) AS time from system.tables GROUP BY database) WHERE time > '" + time + "'" );

        for( String database : lines ) {
            if( "system".equals( database ) ) continue;

            client.useDatabase( database ).dropDatabase();
        }
    }

    public String getClickhouseHost() {
        return clickhouseHost;
    }

    @Override
    protected void after() {
        var client = new DefaultClickhouseClient( clickhouseHost, 8123, databaseName, Dates.m( 1 ), Dates.m( 1 ) );
        dropDatabases( client );

        super.after();
    }

    public String getTestDatabaseName() {
        return testDatabaseName;
    }
}
