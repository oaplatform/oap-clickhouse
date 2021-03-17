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

import oap.application.testng.KernelFixture;
import oap.clickhouse.ClickhouseClient;
import oap.clickhouse.ClickhouseFixture;
import oap.io.Resources;
import oap.testng.Asserts;
import oap.testng.Fixtures;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ClickhouseMigrationTest extends Fixtures {
    private final KernelFixture kernel;
    private final ClickhouseFixture clickhouseFixture;

    public ClickhouseMigrationTest() {
        clickhouseFixture = fixture( new ClickhouseFixture( "aggregator" ) );
        kernel = fixture( new KernelFixture( Resources.filePath( getClass(), "/test-application.conf" ).orElseThrow() ) );
    }

    @Test
    public void testCreateTable() {
        var lines = kernel.service( ClickhouseClient.class ).getLines( "SHOW CREATE TABLE TEST_TABLE" );
        assertThat( lines ).hasSize( 1 );
        Asserts.assertString( lines.get( 0 ) ).isEqualTo( "CREATE TABLE " + clickhouseFixture.getTestDatabaseName() + ".TEST_TABLE\\n"
            + "(\\n"
            + "    `UPLOADTIME` DateTime,\\n"
            + "    `DATETIME` DateTime DEFAULT \\'1970-01-01 00:00:00\\',\\n"
            + "    `TEST_1` String DEFAULT \\'\\',\\n"
            + "    `TEST_2` Int32 DEFAULT 10,\\n"
            + "    INDEX TEST_2 TEST_2 TYPE set(0) GRANULARITY 1\\n"
            + ")\\n"
            + "ENGINE = MergeTree\\n"
            + "PARTITION BY toYYYYMM(DATETIME)\\n"
            + "ORDER BY TEST_1\\n"
            + "SETTINGS index_granularity = 8192" );
    }
}
