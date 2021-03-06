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

import oap.clickhouse.Table.TtlInfo;
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

import static oap.clickhouse.ConfigField.build;
import static oap.clickhouse.ConfigField.buildEnum;
import static oap.clickhouse.ConfigField.buildFixedString;
import static oap.clickhouse.ConfigIndex.bloom_filter;
import static oap.clickhouse.ConfigIndex.index;
import static oap.clickhouse.ConfigIndex.set;
import static oap.clickhouse.Engine.Memory;
import static oap.clickhouse.Engine.MergeTree;
import static oap.clickhouse.FieldType.DATE;
import static oap.clickhouse.FieldType.DATETIME;
import static oap.clickhouse.FieldType.LONG;
import static oap.clickhouse.FieldType.STRING;
import static oap.clickhouse.FieldType.UNSIGNED_INTEGER;
import static oap.testng.Asserts.assertEventually;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TableTest extends DatabaseTest {
    public static final TableEngine TABLE_ENGINE_MEMORY = new TableEngine( Memory );
    private static final TableEngine TABLE_ENGINE = new TableEngine( MergeTree, List.of( "PARTITIONING_DATE" ), List.of( "PARTITIONING_DATE" ), Optional.empty() );

    @Test
    public void testUpgradeInit() {

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
    public void testIndexUpdateNewIndex() {
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
    public void testIndexUpdateDeleteIndex() {
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
    public void testIndexUpdateModifyIndex() {
        var table = database.getTable( "TEST" );
        assertFalse( table.exists() );

        assertTrue( table.upgrade( List.of(
            build( "ID", STRING ).withDefaultValue( "" ),
            build( "ID2", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ),
            List.of(
                index( "ID_ID2", List.of( "ID", "ID2" ), set(), 1 ),
                index( "ID_ID2_bf", List.of( "ID", "ID2" ), bloom_filter(), 1 )
            ),
            TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) )
        );

        assertTrue( table.upgrade( List.of(
            build( "ID", STRING ).withDefaultValue( "" ),
            build( "ID2", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ),
            List.of(
                index( "ID_ID2", List.of( "ID" ), set(), 1 ),
                index( "ID_ID2_bf", List.of( "ID", "ID2" ), bloom_filter(), 1 )
            ),
            TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) )
        );
        assertTrue( table.exists() );
        assertThat( table.getIndexes() ).containsOnly(
            index( "ID_ID2", List.of( "ID" ), set(), 1 ),
            index( "ID_ID2_bf", List.of( "ID", "ID2" ), bloom_filter(), 1 )
        );
    }

    @Test
    public void testUpgradeInitTtl() {
        var table = database.getTable( "TEST" );
        assertFalse( table.exists() );

        assertTrue( table.upgrade( List.of( build( "ID", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ),
            build( "DATE", DATETIME ).withTtl( 100 )
        ), List.of(), TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) ) );

        assertThat( table.getTtlField() ).isEqualTo( new TtlInfo( "DATE", 100 ) );
    }

    @Test
    public void testUpgradeMemoryIgnoreInitTtl() {
        var table = database.getTable( "TEST" );
        assertFalse( table.exists() );

        assertTrue( table.upgrade( List.of( build( "ID", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ),
            build( "DATE", DATETIME ).withTtl( 100 )
        ), List.of(), TABLE_ENGINE_MEMORY, Map.of(), false, Dates.m( 10 ) ) );

        assertNull( table.getTtlField() );
    }

    @Test
    public void testUpgradeUpdateTtl() {
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
    public void testUpgradeMemoryIgnoreUpdateTtl() {
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
    public void testUpgradeAddField() {
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
    public void testUpgradeAddFieldMemoryEngine() {

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
    @SuppressWarnings( "checkstyle:ModifiedControlVariable" )
    public void testFixAlterIsNotFinished() throws InterruptedException {
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

                        assertEventually( 100, 100, () ->
                            assertThat( table.getFields().keySet() )
                                .containsExactly( "ID", "ID2", "ID3", "PARTITIONING_DATE" ) );
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
    public void testUpgradeDropFieldDeny() {
        var table = database.getTable( "TEST" );

        table.upgrade( List.of(
            build( "ID", STRING ).withDefaultValue( "" ),
            build( "ID2", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ), List.of(), TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) );

        assertThatThrownBy( () -> table.upgrade( List.of(
            build( "ID", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ), List.of(), TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) ) ).isInstanceOf( ClickhouseException.class );
    }

    @Test
    public void testUpgradeDropField() {
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

    @Test
    public void testUpgradeReorderFields() {
        setSettings( "prevent_modify", "false" );
        reloadDatabase();

        var table = database.getTable( "TEST" );

        table.upgrade( List.of(
            build( "ID", STRING ).withDefaultValue( "" ),
            build( "ID2", STRING ).withDefaultValue( "" ),
            build( "ID3", STRING ).withDefaultValue( "" ),
            build( "ID4", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ), List.of(), TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) );

        table.refresh();
        assertThat( table.getFields().keySet() ).containsExactly( "ID", "ID2", "ID3", "ID4", "PARTITIONING_DATE" );

        table.upgrade( List.of(
            build( "ID4", STRING ).withDefaultValue( "" ),
            build( "ID2", STRING ).withDefaultValue( "" ),
            build( "ID3", STRING ).withDefaultValue( "" ),
            build( "ID", STRING ).withDefaultValue( "" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ), List.of(), TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) );

        table.refresh();
        assertThat( table.getFields().keySet() ).containsExactly( "ID4", "ID2", "ID3", "ID", "PARTITIONING_DATE" );
    }

    private void setSettings( String name, String value ) {
        database.client.execute( "ALTER TABLE " + SystemSettings.TABLE_SYSTEM_SETTINGS + " UPDATE value = '" + value + "' WHERE name = '" + name + "'", true );
        var lines = List.<String>of();
        do {
            lines = database.client.getLines( "SELECT name FROM " + SystemSettings.TABLE_SYSTEM_SETTINGS + " WHERE name = '" + name + "'", true );

            System.out.println( "values = " + lines );
            Threads.sleepSafely( 100 );
        } while( lines.size() == 1 && lines.get( 0 ).equals( value ) );
    }

    @Test
    public void testUpgradeModifyField() {
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
    public void testUpgradeAlterEnum() {
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
    public void testUpgradeToLowCardinalityPreventModify() {
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
        ).isInstanceOf( ClickhouseException.class );
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
    public void testCodec() {
        setSettings( "prevent_modify", "false" );
        reloadDatabase();

        var table = database.getTable( "TEST" );

        assertTrue( table.upgrade( List.of(
            build( "ID", LONG ).withDefaultValue( 0L ).withCodec( "CODEC(ZSTD(1))" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ), List.of(), TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) ) );


        assertThat( table.getFields().values() ).extracting( tf -> tf.compression_codec )
            .containsExactly( "CODEC(ZSTD(1))", "" );

        assertTrue( table.upgrade( List.of(
            build( "ID", LONG ).withDefaultValue( 0L ).withCodec( "CODEC(DoubleDelta,ZSTD(1))" ),
            build( "PARTITIONING_DATE", DATE ).withDefaultValue( "2019-09-23" ) ), List.of(), TABLE_ENGINE, Map.of(), false, Dates.m( 10 ) ) );


        assertThat( table.getFields().values() ).extracting( tf -> tf.compression_codec )
            .containsExactly( "CODEC(DoubleDelta, ZSTD(1))", "" );
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
            new TableEngine( MergeTree, List.of( "PARTITIONING_DATE" ), List.of( "PARTITIONING_DATE" ), Optional.of( 1024 ) ), Map.of(), false, Dates.m( 10 ) ) );
        assertThat( table.getIndexGranularity() ).isEqualTo( 1024 );


    }
}
