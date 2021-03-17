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

import oap.clickhouse.ViewInfo.Field;
import oap.util.Dates;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static oap.clickhouse.ConfigField.build;
import static oap.clickhouse.Engine.Memory;
import static oap.clickhouse.Engine.MergeTree;
import static oap.clickhouse.FieldType.DATE;
import static oap.clickhouse.FieldType.DATETIME;
import static oap.clickhouse.FieldType.INTEGER;
import static oap.clickhouse.FieldType.LONG;
import static oap.clickhouse.FieldType.STRING;
import static oap.clickhouse.FieldType.STRING_ARRAY;
import static oap.clickhouse.ViewInfo.AggregatorFunction.Function.count;
import static oap.clickhouse.ViewInfo.AggregatorFunction.Function.groupArray;
import static oap.clickhouse.ViewInfo.AggregatorFunction.Function.groupUniqArray;
import static oap.clickhouse.ViewInfo.AggregatorFunction.Function.sum;
import static oap.testng.Asserts.assertString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class ClickhouseDatabaseTest extends DatabaseTest {
    private static final TableEngine TABLE_ENGINE = new TableEngine( MergeTree, List.of( "PARTITIONING_DATE" ), List.of( "PARTITIONING_DATE" ), Optional.empty() );
    private static final TableEngine TABLE_ENGINE3 = new TableEngine( MergeTree, List.of( "PARTITIONING_DATE" ), List.of( "ID", "ID2", "ID3", "SOURCE" ), Optional.empty() );

    @Test
    public void testUpgradeInitTable() {
        database.upgrade( List.of( new TableInfo(
            "TEST",
            List.of( build( "ID", STRING ).withDefaultValue( "" ), build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ),
            List.of(),
            TABLE_ENGINE,
            Map.of() ) ), List.of(), Dates.m( 10 ) );
        assertTrue( database.getTable( "TEST" ).exists() );
    }

    @Test
    public void testUpgradeInitView() {
        database.upgrade( List.of( new TableInfo(
            "TEST",
            List.of(
                build( "ID", STRING ).withDefaultValue( "" ),
                build( "ID2", STRING ).withDefaultValue( "" ),
                build( "ID3", INTEGER ).withDefaultValue( 0 ),
                build( "SOURCE", STRING ).withDefaultValue( 0 ),
                build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" )
            ),
            List.of(),
            TABLE_ENGINE3,
            Map.of() ) ), List.of(), Dates.m( 10 ) );
        assertTrue( database.getTable( "TEST" ).exists() );
        assertFalse( database.getView( "VIEW" ).exists() );

        database.upgrade( List.of( new TableInfo(
            "TEST",
            List.of(
                build( "ID", STRING ).withDefaultValue( "" ),
                build( "ID2", STRING ).withDefaultValue( "" ),
                build( "ID3", INTEGER ).withDefaultValue( 0 ),
                build( "SOURCE", STRING ).withDefaultValue( "" ),
                build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" )
            ),
            List.of(),
            TABLE_ENGINE3,
            Map.of() ) ), List.of(
            new ViewInfo( "VIEW", true, true, List.of( Field.of( "ID" ), Field.of( "ID2" ), Field.of( "PARTITIONING_DATE" ) ),
                List.of( new ViewInfo.AggregatorFunction( null, "C", count ),
                    new ViewInfo.AggregatorFunction( "ID3", "S", sum ),
                    new ViewInfo.AggregatorFunction( "SOURCE", "SOURCE", groupArray ) ),
                Optional.empty(),
                "TEST", Optional.empty() )
        ), Dates.m( 10 ) );
        assertTrue( database.getTable( "TEST" ).exists() );
        assertTrue( database.getView( "VIEW" ).exists() );

        database.client.execute( "INSERT INTO TEST VALUES ('id', 'id2', 3, 'sc1', '2019-11-11'), "
            + "('id', 'id2', 4, 'sc2', '2019-11-11'), "
            + "('id', 'id2', 5, 'sc3', '2019-11-11')", true );

        assertThat( database.client.getLines( "SELECT * FROM VIEW" ) )
            .isEqualTo( List.of( "id\tid2\t2019-11-11\t3\t12\t['sc1','sc2','sc3']" ) );

        var infoTable = database.getTable( "TEST" ).getInfo();
        assertThat( infoTable.dependenciesTable ).isEqualTo( List.of( "VIEW" ) );

        var infoView = database.getTable( "VIEW" ).getInfo();
        assertThat( infoView.engine ).isEqualTo( Engine.MaterializedView );
    }

    @Test
    public void testViewByDateTimeFunction() {
        var tableEngine = new TableEngine( MergeTree, List.of( "PARTITIONING_DATE" ), List.of( "DATETIME", "SOURCE" ), Optional.empty() );
        database.upgrade( List.of( new TableInfo(
            "TEST",
            List.of(
                build( "DATETIME", DATETIME ).withDefaultValue( "2019-20-30 11:10:00" ),
                build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ),
                build( "SOURCE", STRING ).withDefaultValue( "" )
            ),
            List.of(),
            tableEngine,
            Map.of() ) ), List.of(
            new ViewInfo( "VIEW", true, true, List.of( Field.of( "toStartOfDay(DATETIME)", "DATETIME" ), Field.of( "PARTITIONING_DATE" ) ),
                List.of( new ViewInfo.AggregatorFunction( null, "C", count ),
                    new ViewInfo.AggregatorFunction( "SOURCE", "SOURCE", groupUniqArray ) ),
                Optional.empty(),
                "TEST", Optional.empty() )
        ), Dates.m( 10 ) );

        database.client.execute( "INSERT INTO TEST VALUES ('2019-11-11 11:11:11', '2019-11-11', 'sc1'), "
            + "('2019-11-11 12:11:11', '2019-11-11', 'sc1'), "
            + "('2019-11-11 13:11:11', '2019-11-11', 'sc1')", true );
        assertThat( database.client.getLines( "SELECT * FROM VIEW" ) )
            .isEqualTo( List.of( "2019-11-11 00:00:00\t2019-11-11\t3\t['sc1']" ) );
    }

    @Test
    public void testViewWithCustomPK() {
        var tableEngine = new TableEngine( MergeTree, List.of( "PARTITIONING_DATE" ), List.of( "DATETIME", "SOURCE" ), Optional.empty() );
        database.upgrade( List.of( new TableInfo(
            "TEST",
            List.of(
                build( "DATETIME", DATETIME ).withDefaultValue( "2019-20-30 11:10:00" ),
                build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ),
                build( "SOURCE", STRING ).withDefaultValue( "" )
            ),
            List.of(),
            tableEngine,
            Map.of() ) ), List.of(
            new ViewInfo( "VIEW", true, true, List.of( Field.of( "toStartOfDay(DATETIME)", "DATETIME" ), Field.of( "PARTITIONING_DATE" ) ),
                List.of( new ViewInfo.AggregatorFunction( null, "C", count ),
                    new ViewInfo.AggregatorFunction( "SOURCE", "SOURCE", groupUniqArray ) ),
                Optional.empty(),
                "TEST", Optional.empty() ).addPk( "DATETIME" ).addPk( "PARTITIONING_DATE" )
        ), Dates.m( 10 ) );

        assertString( database.client.getLines( "SHOW CREATE TABLE VIEW" ).get( 0 ) )
            .contains( "ORDER BY (DATETIME, PARTITIONING_DATE)" );
    }

    @Test
    public void testViewWithWhere() {
        var tableEngine = new TableEngine( MergeTree, List.of( "PARTITIONING_DATE" ), List.of( "DATETIME", "SOURCE" ), Optional.empty() );
        database.upgrade( List.of( new TableInfo(
            "TEST",
            List.of(
                build( "DATETIME", DATETIME ).withDefaultValue( "2019-20-30 11:10:00" ),
                build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ),
                build( "SOURCE", STRING ).withDefaultValue( "" )
            ),
            List.of(),
            tableEngine,
            Map.of() ) ), List.of(
            new ViewInfo( "VIEW", true, true, List.of( Field.of( "toStartOfDay(DATETIME)", "DATETIME" ), Field.of( "PARTITIONING_DATE" ) ),
                List.of( new ViewInfo.AggregatorFunction( null, "C", count ),
                    new ViewInfo.AggregatorFunction( "SOURCE", "SOURCE", groupUniqArray ) ),
                Optional.of( "DATETIME <> '0000-00-00 00:00:00'" ),
                "TEST", Optional.empty() ).addPk( "DATETIME" ).addPk( "PARTITIONING_DATE" )
        ), Dates.m( 10 ) );

        assertString( database.client.getLines( "SHOW CREATE TABLE VIEW" ).get( 0 ) )
            .contains( "TEST\\nWHERE DATETIME != \\'0000-00-00 00:00:00\\'\\n" );
    }

    @Test
    public void testMemoryTableToViewToTable() {
        var fromTableEngine = new TableEngine( Memory );
        var toTableEngine = new TableEngine( MergeTree, List.of( "PARTITIONING_DATE" ), List.of( "DATETIME" ), Optional.empty() );

        database.upgrade( List.of(
            new TableInfo(
                "FROM_TEST",
                List.of(
                    build( "DATETIME", DATETIME ).withDefaultValue( "2019-20-30 11:10:00" ),
                    build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ),
                    build( "SOURCE", STRING ).withDefaultValue( "" )
                ),
                List.of(),
                fromTableEngine,
                Map.of() ),
            new TableInfo(
                "TO_TEST",
                List.of(
                    build( "DATETIME", DATETIME ).withDefaultValue( "2019-20-30 11:10:00" ),
                    build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ),
                    build( "C", LONG ).withDefaultValue( 0 ),
                    build( "SOURCE", STRING_ARRAY ).withDefaultValue( List.of() )
                ),
                List.of(),
                toTableEngine,
                Map.of() )
            ),

            List.of(
                new ViewInfo( "VIEW", true, false, List.of( Field.of( "toStartOfDay(DATETIME)", "DATETIME" ), Field.of( "PARTITIONING_DATE" ) ),
                    List.of( new ViewInfo.AggregatorFunction( null, "C", count ),
                        new ViewInfo.AggregatorFunction( "SOURCE", "SOURCE", groupUniqArray ) ),
                    Optional.empty(),
                    "FROM_TEST", Optional.of( "TO_TEST" ) )
            ), Dates.m( 10 ) );

        database.client.execute( "INSERT INTO FROM_TEST VALUES ('2019-11-11 11:11:11', '2019-11-11', 'sc1'), "
            + "('2019-11-11 12:11:11', '2019-11-11', 'sc1'), "
            + "('2019-11-11 13:11:11', '2019-11-11', 'sc1')", true );
        assertThat( database.client.getLines( "SELECT * FROM TO_TEST" ) )
            .isEqualTo( List.of( "2019-11-11 00:00:00\t2019-11-11\t3\t['sc1']" ) );
    }
}
