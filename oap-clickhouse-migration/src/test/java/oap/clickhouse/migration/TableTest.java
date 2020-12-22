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

import oap.clickhouse.ClickHouseException;
import oap.clickhouse.DataFormat;
import oap.clickhouse.SystemSettings;
import oap.clickhouse.migration.Table.TtlInfo;
import oap.concurrent.Executors;
import oap.concurrent.Threads;
import oap.util.Dates;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static oap.clickhouse.migration.ConfigField.build;
import static oap.clickhouse.migration.ConfigField.buildEnum;
import static oap.clickhouse.migration.ConfigField.buildFixedString;
import static oap.clickhouse.migration.ConfigIndex.index;
import static oap.clickhouse.migration.ConfigIndex.set;
import static oap.clickhouse.migration.Engine.Memory;
import static oap.clickhouse.migration.Engine.MergeTree;
import static oap.clickhouse.migration.FieldType.DATE;
import static oap.clickhouse.migration.FieldType.DATETIME;
import static oap.clickhouse.migration.FieldType.STRING;
import static oap.clickhouse.migration.FieldType.UNSIGNED_INTEGER;
import static oap.testng.Asserts.assertEventually;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Created by igor.petrenko on 28.02.2018.
 */
public class TableTest extends DatabaseTest {
    public static final TableEngine TABLE_ENGINE_MEMORY = new TableEngine( Memory );
    private static final TableEngine TABLE_ENGINE = new TableEngine( MergeTree, "PARTITIONING_DATE", List.of( "PARTITIONING_DATE" ), Optional.empty() );

    @Test
    public void testUpgrade_Init() {

        var table = database.getTable( "TEST" );
        assertFalse( table.exists() );

        assertTrue( table.upgrade( List.of( build( "ID", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ), List.of(), TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) ) );
        assertTrue( table.exists() );

        assertFalse( table.upgrade( List.of( build( "ID", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ), List.of(), TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) ) );
        assertTrue( table.exists() );
    }

    @Test
    public void testInitIndex() {
        var table = database.getTable( "TEST" );
        assertFalse( table.exists() );

        assertTrue( table.upgrade( List.of(
            build( "ID", STRING ).withDefaultValue( "" ),
            build( "ID2", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ),
            List.of( index( "ID_ID2", List.of( "ID", "ID2" ), set(), 1 ) ),
            TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) )
        );
        assertTrue( table.exists() );
        assertThat( table.getIndexes() ).containsOnlyOnce( index( "ID_ID2", List.of( "ID", "ID2" ), set(), 1 ) );

        assertFalse( table.upgrade( List.of(
            build( "ID", STRING ).withDefaultValue( "" ),
            build( "ID2", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ),
            List.of( index( "ID_ID2", List.of( "ID", "ID2" ), set(), 1 ) ),
            TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) )
        );
    }

    @Test
    public void testIndex_Update_NewIndex() {
        var table = database.getTable( "TEST" );
        assertFalse( table.exists() );

        assertTrue( table.upgrade( List.of(
            build( "ID", STRING ).withDefaultValue( "" ),
            build( "ID2", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ),
            List.of(),
            TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) )
        );

        assertTrue( table.upgrade( List.of(
            build( "ID", STRING ).withDefaultValue( "" ),
            build( "ID2", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ),
            List.of( index( "ID_ID2", List.of( "ID", "ID2" ), set(), 1 ) ),
            TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) )
        );
        assertTrue( table.exists() );
        assertThat( table.getIndexes() ).containsOnlyOnce( index( "ID_ID2", List.of( "ID", "ID2" ), set(), 1 ) );
    }

    @Test
    public void testIndex_Update_DeleteIndex() {
        var table = database.getTable( "TEST" );
        assertFalse( table.exists() );

        assertTrue( table.upgrade( List.of(
            build( "ID", STRING ).withDefaultValue( "" ),
            build( "ID2", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ),
            List.of( index( "ID_ID2", List.of( "ID", "ID2" ), set(), 1 ) ),
            TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) )
        );

        assertTrue( table.upgrade( List.of(
            build( "ID", STRING ).withDefaultValue( "" ),
            build( "ID2", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ),
            List.of(),
            TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) )
        );
        assertTrue( table.exists() );
        assertThat( table.getIndexes() ).isEmpty();
    }

    @Test
    public void testIndex_Update_ModifyIndex() {
        var table = database.getTable( "TEST" );
        assertFalse( table.exists() );

        assertTrue( table.upgrade( List.of(
            build( "ID", STRING ).withDefaultValue( "" ),
            build( "ID2", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ),
            List.of( index( "ID_ID2", List.of( "ID", "ID2" ), set(), 1 ) ),
            TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) )
        );

        assertTrue( table.upgrade( List.of(
            build( "ID", STRING ).withDefaultValue( "" ),
            build( "ID2", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ),
            List.of( index( "ID_ID2", List.of( "ID" ), set(), 1 ) ),
            TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) )
        );
        assertTrue( table.exists() );
        assertThat( table.getIndexes() ).containsOnlyOnce( index( "ID_ID2", List.of( "ID" ), set(), 1 ) );
    }

    @Test
    public void testUpgrade_InitTtl() {
        var table = database.getTable( "TEST" );
        assertFalse( table.exists() );

        assertTrue( table.upgrade( List.of( build( "ID", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ),
            build( "DATE", DATETIME ).withTtl( 100 )
        ), List.of(), TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) ) );

        assertThat( table.getTtlField() ).isEqualTo( new TtlInfo( "DATE", 100 ) );
    }

    @Test
    public void testUpgrade_Memory_IgnoreInitTtl() {
        var table = database.getTable( "TEST" );
        assertFalse( table.exists() );

        assertTrue( table.upgrade( List.of( build( "ID", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ),
            build( "DATE", DATETIME ).withTtl( 100 )
        ), List.of(), TABLE_ENGINE_MEMORY, Map.of(), false, Dates.m( 10 ) ) );

        assertNull( table.getTtlField() );
    }

    @Test
    public void testUpgrade_UpdateTtl() {
        var table = database.getTable( "TEST" );
        assertFalse( table.exists() );

        assertTrue( table.upgrade( List.of( build( "ID", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ),
            build( "DATE", DATETIME )
        ), List.of(), TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) ) );

        assertNull( table.getTtlField() );

        assertTrue( table.upgrade( List.of( build( "ID", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ),
            build( "DATE", DATETIME ).withTtl( 100 )
        ), List.of(), TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) ) );
        assertThat( table.getTtlField() ).isEqualTo( new TtlInfo( "DATE", 100 ) );

        assertFalse( table.upgrade( List.of( build( "ID", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ),
            build( "DATE", DATETIME ).withTtl( 100 )
        ), List.of(), TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) ) );
        assertThat( table.getTtlField() ).isEqualTo( new TtlInfo( "DATE", 100 ) );

        assertTrue( table.upgrade( List.of( build( "ID", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ),
            build( "DATE", DATETIME ).withTtl( 200 )
        ), List.of(), TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) ) );
        assertThat( table.getTtlField() ).isEqualTo( new TtlInfo( "DATE", 200 ) );

        assertTrue( table.upgrade( List.of( build( "ID", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ),
            build( "DATE", DATETIME )
        ), List.of(), TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) ) );

        assertNull( table.getTtlField() );
    }

    @Test
    public void testUpgrade_Memory_IgnoreUpdateTtl() {
        var table = database.getTable( "TEST" );
        assertFalse( table.exists() );

        assertTrue( table.upgrade( List.of( build( "ID", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ),
            build( "DATE", DATETIME )
        ), List.of(), TABLE_ENGINE_MEMORY, Map.of(), false, Dates.m( 10 ) ) );

        assertNull( table.getTtlField() );

        assertTrue( table.upgrade( List.of( build( "ID", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ),
            build( "DATE", DATETIME ).withTtl( 100 )
        ), List.of(), TABLE_ENGINE_MEMORY, Map.of(), false, Dates.m( 10 ) ) );

        assertNull( table.getTtlField() );
    }

    @Test
    public void testUpgrade_AddField() throws Exception {
        var table = database.getTable( "TEST" );

        table.upgrade( List.of(
            build( "ID", STRING ).withDefaultValue( "" ),
            build( "ID2", STRING ).withDefaultValue( "" ),
            build( "ID3", UNSIGNED_INTEGER ).withDefaultValue( 0 ),
            buildFixedString( "ID4", 2 ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ), List.of(), TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) );

        assertThat( table.getFields().keySet() ).containsExactly( "ID", "ID2", "ID3", "ID4", "PARTITIONING_DATE" );
        assertThat( table.getFields().values() ).extracting( tf -> tf.type ).containsExactly( "String", "String", "UInt32", "FixedString(2)", "Date" );
    }

    @Test
    public void testUpgrade_AddField_MemoryEngine() throws Exception {

        var table = database.getTable( "TEST" );

        table.upgrade( List.of(
            build( "ID", STRING ).withDefaultValue( "" ),
            build( "ID2", STRING ).withDefaultValue( "" ),
            build( "ID3", UNSIGNED_INTEGER ).withDefaultValue( 0 ),
            buildFixedString( "ID4", 2 ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ), List.of(), TABLE_ENGINE_MEMORY, Map.of(), false, Dates.m( 10 ) );

        assertTrue( table.isMemoryEngine() );

        assertThat( table.getFields().keySet() ).containsExactly( "ID", "ID2", "ID3", "ID4", "PARTITIONING_DATE" );
        assertThat( table.getFields().values() ).extracting( tf -> tf.type ).containsExactly( "String", "String", "UInt32", "FixedString(2)", "Date" );
    }

    @Test
    public void testFixAlter_is_not_finished() throws InterruptedException {
        var executorService = Executors.newFixedThreadPool( 2 );
        try {
            for( var i = 0; i < 100; i++ ) {
                var finalI = i;
                try {
                    executorService.execute( () -> {
                        var table = database.getTable( "TEST" + finalI );

                        table.upgrade( List.of(
                            build( "ID", STRING ).withDefaultValue( "" ),
                            build( "ID2", STRING ).withDefaultValue( "" ),
                            build( "ID3", STRING ).withDefaultValue( "" ),
                            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ), List.of(), TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) );

                        assertEventually( 100, 100, () -> {
                            assertThat( table.getFields().keySet() )
                                .containsExactly( "ID", "ID2", "ID3", "PARTITIONING_DATE" );
                        } );
                    } );
                } catch( RejectedExecutionException e ) {
                    Threads.sleepSafely( 100 );
                    i--;
                }
            }
        } finally {
            executorService.shutdown();
            executorService.awaitTermination( 1, TimeUnit.MINUTES );
        }
    }

    @Test
    public void testUpgrade_DropField_Deny() throws Exception {
        var table = database.getTable( "TEST" );

        table.upgrade( List.of(
            build( "ID", STRING ).withDefaultValue( "" ),
            build( "ID2", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ), List.of(), TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) );

        assertThatThrownBy( () -> table.upgrade( List.of(
            build( "ID", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ), List.of(), TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) ) ).isInstanceOf( ClickHouseException.class );
    }

    @Test
    public void testUpgrade_DropField() throws Exception {
        setSettings( "prevent_destroy", "false" );
        reloadDatabase();

        var table = database.getTable( "TEST" );

        table.upgrade( List.of(
            build( "ID", STRING ).withDefaultValue( "" ),
            build( "ID2", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ), List.of(), TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) );

        table.upgrade( List.of(
            build( "ID", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ), List.of(), TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) );

        assertThat( table.getFields().keySet() ).containsExactly( "ID", "PARTITIONING_DATE" );

        var lines = database.client.getLines( "SELECT value FROM " + SystemSettings.TABLE_SYSTEM_SETTINGS + " WHERE name = 'prevent_destroy'" );
        assertThat( lines ).containsExactly( "true" );
    }

    private void setSettings( String name, String value ) {
        database.client.execute( "ALTER TABLE " + SystemSettings.TABLE_SYSTEM_SETTINGS + " UPDATE value = '" + value + "' WHERE name = '" + name + "'", true );
    }

    @Test
    public void testUpgrade_ModifyField() throws Exception {
        setSettings( "prevent_modify", "false" );
        reloadDatabase();

        var table = database.getTable( "TEST" );

        table.upgrade( List.of(
            build( "ID", STRING ).withDefaultValue( "" ),
            build( "ID2", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ), List.of(), TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) );

        assertThat( table.getFields().values() ).extracting( tf -> tf.type ).containsExactly( "String", "String", "Date" );

        table.upgrade( List.of(
            build( "ID", STRING ).withDefaultValue( "" ),
            buildFixedString( "ID2", 10 ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ), List.of(), TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) );

        assertThat( table.getFields().values() ).extracting( tf -> tf.type ).containsExactly( "String", "FixedString(10)", "Date" );
    }

    @Test
    public void testUpgrade_AlterEnum() throws Exception {
        setSettings( "prevent_modify", "false" );
        reloadDatabase();

        var table = database.getTable( "TEST" );

        assertTrue( table.upgrade( List.of(
            build( "ID", STRING ).withDefaultValue( "" ),
            buildEnum( "ID2", "test-dictionary" ).withDefaultValue( "id1" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ), List.of(), TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) ) );
        assertThat( table.getFields().values() ).extracting( tf -> tf.type ).containsExactly( "String", "Enum8('id1' = 1, 'id2' = 2, 'kid3' = 3, 'did3$' = 4)", "Date" );

        assertTrue( table.upgrade( List.of(
            build( "ID", STRING ).withDefaultValue( "" ),
            buildEnum( "ID2", "test-dictionary2" ).withDefaultValue( "id2" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ), List.of(), TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) ) );
        assertThat( table.getFields().values() ).extracting( tf -> tf.type ).containsExactly( "String", "Enum8('id2' = 2, 'kid3' = 3, 'did4' = 4)", "Date" );

        assertFalse( table.upgrade( List.of(
            build( "ID", STRING ).withDefaultValue( "" ),
            buildEnum( "ID2", "test-dictionary2" ).withDefaultValue( "id2" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ), List.of(), TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) ) );
        assertThat( table.getFields().values() ).extracting( tf -> tf.type ).containsExactly( "String", "Enum8('id2' = 2, 'kid3' = 3, 'did4' = 4)", "Date" );

        var lines = database.client.getLines( "SELECT value FROM " + SystemSettings.TABLE_SYSTEM_SETTINGS + " WHERE name = 'prevent_modify'" );
        assertThat( lines ).containsExactly( "true" );
    }

    @Test
    public void testUpgradeToLowCardinality_preventModify() {
        var table = database.getTable( "TEST" );

        table.upgrade( List.of(
            build( "ID", STRING ).withDefaultValue( "" ),
            build( "ID2", STRING, false ).withDefaultValue( "id1" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ), List.of(), TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) );

        assertThatThrownBy( () ->
            assertTrue( table.upgrade( List.of(
                build( "ID", STRING ).withDefaultValue( "" ),
                build( "ID2", STRING, true ).withDefaultValue( "id1" ),
                build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ), List.of(), TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) ) )
        ).isInstanceOf( ClickHouseException.class );
    }

    @Test
    public void testUpgradeToLowCardinality() {
        setSettings( "prevent_modify", "false" );
        reloadDatabase();

        var table = database.getTable( "TEST" );

        assertTrue( table.upgrade( List.of(
            build( "ID", STRING ).withDefaultValue( "" ),
            build( "ID2", STRING, false ).withDefaultValue( "id1" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ), List.of(), TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) ) );
        assertThat( table.getFields().values() ).extracting( tf -> tf.type ).containsExactly( "String", "String", "Date" );

        assertTrue( table.upgrade( List.of(
            build( "ID", STRING ).withDefaultValue( "" ),
            build( "ID2", STRING, true ).withDefaultValue( "id1" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ), List.of(), TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) ) );
        assertThat( table.getFields().values() ).extracting( tf -> tf.type ).containsExactly( "String", "LowCardinality(String)", "Date" );

        assertFalse( table.upgrade( List.of(
            build( "ID", STRING ).withDefaultValue( "" ),
            build( "ID2", STRING, true ).withDefaultValue( "id1" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ), List.of(), TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) ) );
        assertThat( table.getFields().values() ).extracting( tf -> tf.type ).containsExactly( "String", "LowCardinality(String)", "Date" );
    }

    @Test
    public void testNoReorderFields() {
        var table = database.getTable( "TEST" );

        table.upgrade( List.of(
            build( "ID", STRING ).withDefaultValue( "" ),
            build( "ID2", STRING ).withDefaultValue( "" ),
            build( "ID3", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ), List.of(), TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) );

        assertThat( table.getFields().keySet() ).containsExactly( "ID", "ID2", "ID3", "PARTITIONING_DATE" );

        table.upgrade( List.of(
            build( "ID", STRING ).withDefaultValue( "" ),
            build( "ID3", STRING ).withDefaultValue( "" ),
            build( "ID2", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ), List.of(), TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) );

        assertThat( table.getFields().keySet() ).containsExactly( "ID", "ID2", "ID3", "PARTITIONING_DATE" );
    }

    @Test
    public void testTruncateInMemoryTable() throws IOException {
        var table = database.getTable( "TEST" );
        table.upgrade( List.of( build( "ID", STRING ).withDefaultValue( "" ) ), List.of(), TABLE_ENGINE_MEMORY, Map.of(), false, Dates.m( 10 ) );

        try( var out = table.database.client.put( "TEST", DataFormat.TabSeparated ) ) {
            out.write( "11\n".getBytes() );
            out.write( "12".getBytes() );
        }

        assertThat( table.database.client.getLines( "SELECT * FROM TEST" ) ).hasSize( 2 );

        table.truncate();
        assertThat( table.database.client.getLines( "SELECT * FROM TEST" ) ).isEmpty();
    }

    @Test
    public void testIndexGranularity() {
        var table = database.getTable( "TEST" );

        assertTrue( table.upgrade( List.of( build( "ID", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ), List.of(),
            TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) ) );
        assertThat( table.getIndexGranularity() ).isEqualTo( 8192 );

        table = database.getTable( "TEST2" );

        assertTrue( table.upgrade( List.of( build( "ID", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ), List.of(),
            new TableEngine( MergeTree, "PARTITIONING_DATE", List.of( "PARTITIONING_DATE" ), Optional.of( 1024 ) ), Map.of(), false, Dates.m( 10 ) ) );
        assertThat( table.getIndexGranularity() ).isEqualTo( 1024 );


    }
}
