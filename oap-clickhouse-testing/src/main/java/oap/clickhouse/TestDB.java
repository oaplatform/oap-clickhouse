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

import oap.testng.Teamcity;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;

import java.util.List;

/**
 * Created by igor.petrenko on 02.03.2018.
 */
public class TestDB {
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

    public static String testDbName( String database ) {
        return StringUtils.replaceChars( database + "_" + Teamcity.buildPrefix(), ".-", "_" );
    }
}
