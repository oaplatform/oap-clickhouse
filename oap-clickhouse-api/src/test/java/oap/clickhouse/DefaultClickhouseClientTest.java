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
import oap.testng.Teamcity;
import oap.util.Dates;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static oap.clickhouse.ConfigField.build;
import static oap.clickhouse.ConfigField.buildFixedArrayString;
import static oap.clickhouse.Engine.MergeTree;
import static oap.clickhouse.FieldType.DATE;
import static oap.clickhouse.FieldType.STRING;
import static oap.testng.Asserts.assertString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DefaultClickhouseClientTest {
    private static final TableEngine TABLE_ENGINE = new TableEngine( MergeTree, List.of( "PARTITIONING_DATE" ), List.of( "PARTITIONING_DATE" ), Optional.empty() );

    private static final String HOST = Env.get( "CLICKHOUSE_HOST", "localhost" );
    private static final int PORT = 8123;
    private static final int TIMEOUT = 1048576;

    private String DB;
    private Database database;
    private String initSql;
    private DefaultClickhouseClient clickHouseClient;

    @BeforeMethod
    public void beforeMethod() {
        DB = "db_" + StringUtils.replaceChars( Teamcity.buildPrefix(), ".-", "_" );
        System.setProperty( "TABLE_SUFFIX", "_" + RandomStringUtils.randomAlphabetic( 5 ) );
        database = new DefaultClickhouseClient( HOST, PORT, DB, TIMEOUT, TIMEOUT ).getDatabase();

        clickHouseClient = new DefaultClickhouseClient( HOST, PORT, DB, TIMEOUT, TIMEOUT );
        clickHouseClient.start();

        try {
            clickHouseClient.dropDatabase();
        } catch( Exception ignored ) {
        }
    }

    @AfterMethod
    public void afterMethod() {
        try {
            clickHouseClient.dropDatabase();
        } catch( Exception ignored ) {
        }
    }

    @Test
    public void testClickhouseNotFound() {
        assertThatCode( () -> new DefaultClickhouseClient( "unknown host", 9999, DB, TIMEOUT, TIMEOUT ) )
            .doesNotThrowAnyException();
    }

    @Test
    public void testCreateDatabaseAndTable() {
        clickHouseClient.createDatabase();
        var table = database.getTable( "TEST" );
        database.upgrade( List.of( new TableInfo( "TEST", List.of(
            build( "ID", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ), List.of(), TABLE_ENGINE, Map.of() ) ), List.of(),
            Dates.m( 10 ) );
    }

    @Test
    public void testArray() throws IOException {
        clickHouseClient.createDatabase();
        var table = database.getTable( "TEST" );
        database.upgrade( List.of( new TableInfo( "TEST", List.of(
            build( "ID", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ),
            buildFixedArrayString( "test_array", 2 ).withDefaultValue( List.of() ) ), List.of(), TABLE_ENGINE, Map.of() ) ), List.of(),
            Dates.m( 10 ) );

        try( var out = clickHouseClient.put( "TEST", DataFormat.TabSeparated ) ) {
            out.write( "11\t2017-01-01\t['11','22','33']\n".getBytes() );
            out.write( "12\t2017-01-01\t['aa','bb','cc']".getBytes() );
        }
        var lines = clickHouseClient.getLines( "SELECT * FROM TEST" );
        assertThat( lines ).containsOnlyOnce( "11\t2017-01-01\t['11','22','33']", "12\t2017-01-01\t['aa','bb','cc']" );
    }

    @Test
    public void testSubstitute() {
        clickHouseClient.createDatabase();
        var table = database.getTable( "TEST" );

        database.upgrade( List.of( new TableInfo( "TEST", List.of(
            build( "ID", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ),
            buildFixedArrayString( "test_array", 2 ).withDefaultValue( List.of() ) ), List.of(), TABLE_ENGINE, Map.of() ) ), List.of(),
            Dates.m( 10 ) );

        clickHouseClient.execute( "ALTER TABLE TEST DELETE WHERE ID IN ('${TEST}')", true );
    }

    @Test( dependsOnMethods = "testCreateDatabaseAndTable" )
    public void testPut() throws IOException {
        testCreateDatabaseAndTable();

        try( var out = clickHouseClient.put( "TEST", DataFormat.TabSeparated ) ) {
            out.write( "11\t2017-01-01\n".getBytes() );
            out.write( "12\t2017-01-01".getBytes() );
        }
        var lines = clickHouseClient.getLines( "SELECT * FROM TEST" );
        assertThat( lines ).containsOnlyOnce( "11\t2017-01-01", "12\t2017-01-01" );

        var count = clickHouseClient.get( "SELECT * FROM TEST FORMAT TabSeparatedWithNames", line -> {} );
        assertThat( count ).isEqualTo( 3 );
    }

    @Test( dependsOnMethods = "testCreateDatabaseAndTable" )
    public void testCSVWithNames() throws IOException {
        testCreateDatabaseAndTable();

        try( var out = clickHouseClient.put( "TEST", DataFormat.TabSeparated ) ) {
            out.write( "11\t2017-01-01\n".getBytes() );
            out.write( "12\t2017-01-01".getBytes() );
        }
        var lines = String.join( "\n", clickHouseClient.getLines( "SELECT * FROM TEST FORMAT " + DataFormat.CSVWithNames ) );
        assertString( lines ).isEqualTo( "\"ID\",\"PARTITIONING_DATE\"\n"
            + "\"11\",\"2017-01-01\"\n"
            + "\"12\",\"2017-01-01\"" );
    }

    @Test( dependsOnMethods = "testCreateDatabaseAndTable" )
    public void testPutFailed() throws IOException {
        testCreateDatabaseAndTable();

        assertThatThrownBy( () -> {
            try( var out = clickHouseClient.put( "TEST", DataFormat.TabSeparated ) ) {
                out.write( "11\t2017-01-01 10:12:13\n".getBytes() );
                out.write( "12\t2017-01-01".getBytes() );
            }
        } ).isInstanceOf( ClickhouseException.class );
    }

    @Test
    public void testDelete() throws IOException {
        clickHouseClient.createDatabase();
        database.upgrade( List.of( new TableInfo( "TEST", List.of(
            build( "ID", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ), List.of(), TABLE_ENGINE, Map.of() ) ), List.of(),
            Dates.m( 10 ) );

        try( var out = clickHouseClient.put( "TEST", DataFormat.TabSeparated ) ) {
            out.write( "11\t2017-01-01\n".getBytes() );
            out.write( "12\t2017-01-01\n".getBytes() );
        }

        try( var out = clickHouseClient.put( "TEST", DataFormat.TabSeparated ) ) {
            out.write( "11\t2017-01-01\n".getBytes() );
            out.write( "11\t2017-01-01\n".getBytes() );
            out.write( "11\t2017-01-01\n".getBytes() );
            out.write( "11\t2017-01-01\n".getBytes() );
            out.write( "11\t2017-01-01\n".getBytes() );
            out.write( "11\t2017-01-01\n".getBytes() );
            out.write( "11\t2017-01-01\n".getBytes() );
            out.write( "11\t2017-01-01\n".getBytes() );
        }

        clickHouseClient.deleteRows( "TEST", "ID   IN ('11')", false );
        var lines = clickHouseClient.getLines( "SELECT * FROM TEST" );
        assertThat( lines ).containsOnlyOnce( "12\t2017-01-01" );
    }
}
