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

import static oap.testng.Fixture.Scope.CLASS;
import static oap.testng.Fixture.Scope.METHOD;
import static oap.testng.Fixture.Scope.SUITE;

/**
 * Created by igor.petrenko on 2020-12-04.
 */
public class ClickhouseFixture extends EnvFixture {
    private final String CLICKHOUSE_HOST = Env.get( "CLICKHOUSE_HOST", "localhost" );
    private final Scope scope;
    private final String databaseName;

    public ClickhouseFixture( String databaseName ) {
        this( METHOD, databaseName );
    }

    public ClickhouseFixture( Scope scope, String databaseName ) {
        this.scope = scope;
        this.databaseName = databaseName;
    }

    public static String testDbName( String database ) {
        return StringUtils.replaceChars( database + "_" + Teamcity.buildPrefix(), ".-", "_" );
    }

    public static void dropDatabases( ClickHouseClient client ) {
        final String time = DateTime.now().minusDays( 2 ).toString( "YYYY-MM-dd HH:mm:ss" );
        final List<String> lines = client
            .useDatabase( "system" )
            .getLines( "SELECT database FROM (SELECT database, MAX(metadata_modification_time) AS time from system.tables GROUP BY database) WHERE time > '" + time + "'" );

        System.out.println( "lines = " + lines );

        for( String database : lines ) {
            if( "system".equals( database ) ) continue;

            client.useDatabase( database ).dropDatabase();
        }
    }

    private void defineEnv() {
        define( scope, "DATABASE_NAME", testDbName( databaseName ) );
        define( "CLICKHOUSE_HOST", CLICKHOUSE_HOST );
    }

    public ClickhouseFixture withScope( Scope scope ) {
        return new ClickhouseFixture( scope, this.databaseName );
    }

    @Override
    public void afterMethod() {
        stop( METHOD );
        super.afterMethod();
    }

    @Override
    public void afterClass() {
        stop( CLASS );
        super.afterClass();
    }

    @Override
    public void afterSuite() {
        stop( SUITE );
        super.afterSuite();
    }

    public void stop( Scope scope ) {
        if( scope == this.scope ) {
            var client = new DefaultClickHouseClient( CLICKHOUSE_HOST, 8123, databaseName, Dates.m( 1 ), Dates.m( 1 ) );
            dropDatabases( client );
        }
    }
}
