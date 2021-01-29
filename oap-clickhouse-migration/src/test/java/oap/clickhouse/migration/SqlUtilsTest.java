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

import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static java.util.Arrays.asList;
import static oap.clickhouse.migration.Engine.MergeTree;
import static oap.testng.Asserts.assertString;

public class SqlUtilsTest {
    @Test
    public void testAddFieldsToInitQuery() {
        var fields = List.of(
            new ConfigField( "ID", FieldType.STRING, Optional.empty(), Optional.empty(), "", Optional.empty(), Optional.empty(), Optional.of( "" ), 0 ),
            new ConfigField( "AFTER_ID", FieldType.STRING, Optional.empty(), Optional.empty(), "", Optional.empty(), Optional.empty(), Optional.of( "" ), 0 ),
            new ConfigField( "PARTITIONING_DATE", FieldType.DATE, Optional.empty(), Optional.empty(), "", Optional.empty(), Optional.empty(), Optional.of( "2019-09-23" ), 0 ),
            new ConfigField( "AFTER_PARTITIONING_DATE", FieldType.STRING, Optional.empty(), Optional.empty(), "", Optional.empty(), Optional.empty(), Optional.of( "" ), 0 ),
            new ConfigField( "TEST_TIME", FieldType.DATETIME, Optional.empty(), Optional.empty(), "", Optional.empty(), Optional.of( "now()" ), Optional.of( "2019-09-23 00:00:00" ), 0 ),
            new ConfigField( "TIME", FieldType.DATETIME, Optional.empty(), Optional.empty(), "", Optional.empty(), Optional.of( "now()" ), Optional.of( "2019-09-23 00:00:00" ), 0 ),
            new ConfigField( "TIME2", FieldType.DATETIME, Optional.empty(), Optional.empty(), "", Optional.empty(), Optional.of( "now()" ), Optional.of( "2019-09-23 00:00:00" ), 0 ),
            new ConfigField( "LC", FieldType.STRING, Optional.empty(), Optional.of( true ), "", Optional.empty(), Optional.empty(), Optional.of( "V" ), 0 )
        );

        var sql = SqlUtils.addFieldsIndexesToInitQuery( new TableEngine( MergeTree, List.of( "PARTITIONING_DATE" ), List.of( "ID" ), Optional.empty() ), fields, List.of() );

        assertString( sql ).isEqualTo( """
            CREATE TABLE ${TABLE}${THREAD_IDX} (
            ID String DEFAULT '',
            AFTER_ID String DEFAULT '',
            PARTITIONING_DATE Date DEFAULT '2019-09-23',
            AFTER_PARTITIONING_DATE String DEFAULT '',
            TEST_TIME DateTime MATERIALIZED now(),
            TIME DateTime MATERIALIZED now(),
            TIME2 DateTime MATERIALIZED now(),
            LC LowCardinality(String) DEFAULT 'V'
            ) Engine = MergeTree PARTITION BY (PARTITIONING_DATE) ORDER BY (ID)""".stripIndent() );
    }

    @Test
    public void testAddFieldsToInitQuery_length() {
        var fields = asList(
            new ConfigField( "ID", FieldType.STRING, Optional.empty(), Optional.empty(), "", Optional.empty(), Optional.empty(), Optional.of( "" ), 0 ),
            new ConfigField( "AFTER_ID", FieldType.STRING, Optional.of( 5 ), Optional.empty(), "", Optional.empty(), Optional.empty(), Optional.of( "" ), 0 ),
            new ConfigField( "AFTER_AFTER_ID", FieldType.STRING, Optional.of( 6 ), Optional.empty(), "", Optional.empty(), Optional.empty(), Optional.of( "" ), 0 ),
            new ConfigField( "PARTITIONING_DATE", FieldType.DATE, Optional.empty(), Optional.empty(), "", Optional.empty(), Optional.empty(), Optional.of( "2019-09-23" ), 0 )
        );

        var sql = SqlUtils.addFieldsIndexesToInitQuery( new TableEngine( MergeTree,
            List.of( "PARTITIONING_DATE" ), List.of( "ID" ), Optional.empty() ), fields, List.of() );

        assertString( sql ).isEqualTo( """
            CREATE TABLE ${TABLE}${THREAD_IDX} (
            ID String DEFAULT '',
            AFTER_ID FixedString(5) DEFAULT '',
            AFTER_AFTER_ID FixedString(6) DEFAULT '',
            PARTITIONING_DATE Date DEFAULT '2019-09-23'
            ) Engine = MergeTree PARTITION BY (PARTITIONING_DATE) ORDER BY (ID)""".strip() );
    }

    @Test
    public void testAddFieldsToInitQuery_enums() {
        var fields = asList(
            new ConfigField( "ID", FieldType.STRING, Optional.empty(), Optional.empty(), "", Optional.empty(), Optional.empty(), Optional.of( "" ), 0 ),
            new ConfigField( "AFTER_ID", FieldType.ENUM, Optional.empty(), Optional.empty(), "", Optional.of( "test-dictionary" ), Optional.empty(), Optional.of( "id1" ), 0 ),
            new ConfigField( "AFTER_AFTER_ID", FieldType.ENUM, Optional.empty(), Optional.empty(), "", Optional.of( "test-dictionary" ), Optional.empty(), Optional.of( "id1" ), 0 ),
            new ConfigField( "PARTITIONING_DATE", FieldType.DATE, Optional.empty(), Optional.empty(), "", Optional.empty(), Optional.empty(), Optional.of( "2019-09-23" ), 0 )
        );

        var sql = SqlUtils.addFieldsIndexesToInitQuery( new TableEngine( MergeTree, List.of( "PARTITIONING_DATE" ), List.of( "ID" ), Optional.empty() ), fields, List.of() );

        assertString( sql ).isEqualTo( """
            CREATE TABLE ${TABLE}${THREAD_IDX} (
            ID String DEFAULT '',
            AFTER_ID Enum8('id1' = 1, 'id2' = 2, 'kid3' = 3, 'did3$' = 4) DEFAULT 'id1',
            AFTER_AFTER_ID Enum8('id1' = 1, 'id2' = 2, 'kid3' = 3, 'did3$' = 4) DEFAULT 'id1',
            PARTITIONING_DATE Date DEFAULT '2019-09-23'
            ) Engine = MergeTree PARTITION BY (PARTITIONING_DATE) ORDER BY (ID)""".stripIndent() );
    }

}
