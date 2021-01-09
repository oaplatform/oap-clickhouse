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

package oap.clickhouse.migration;

import oap.clickhouse.DefaultClickhouseClient;
import oap.system.Env;
import oap.testng.Teamcity;
import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public class DatabaseTest {
    public static final String DB = "table_db_" + StringUtils.replaceChars( Teamcity.buildPrefix(), ".-", "_" );
    private static final String HOST = Env.get( "CLICKHOUSE_HOST", "localhost" );
    protected Database database;

    @BeforeMethod
    public void beforeMethod() {
        reloadDatabase();
    }

    protected void reloadDatabase() {
        var clickHouseClient = new DefaultClickhouseClient( HOST, 8123, DB );
        clickHouseClient.start();
        database = clickHouseClient.getDatabase();
        database.createIfNotExists();
        if( database.getTable( "TEST" ).exists() )
            database.getTable( "TEST" ).drop();
    }

    @AfterMethod
    public void afterMethod() {
        try {
            database.drop();
        } catch( Exception e ) {
            e.printStackTrace();
        }
    }
}
