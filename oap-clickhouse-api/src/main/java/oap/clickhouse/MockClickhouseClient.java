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

import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang3.NotImplementedException;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Collections.emptyList;

public class MockClickhouseClient implements ClickhouseClient {
    public final HashSet<Drop> drops;
    public final HashSet<Put> puts;
    public final List<String> execute;
    private final String database;
    private List<String> lines = emptyList();
    private List<String> getLines = emptyList();

    public MockClickhouseClient() {
        this( "mock", new HashSet<>(), new HashSet<>(), new ArrayList<>() );
    }

    public MockClickhouseClient( String database, HashSet<Drop> drops, HashSet<Put> puts, List<String> execute ) {
        this.database = database;
        this.drops = drops;
        this.puts = puts;
        this.execute = execute;
    }

    @Override
    public List<String> getLines( String query, boolean useDatabase, long timeout ) {
        return null;
    }

    @Override
    public OutputStream put( String table, DataFormat format, long timeout ) {
        return put( table, format, List.of(), timeout );
    }

    @Override
    public OutputStream put( String table, DataFormat format, Collection<String> fields, long timeout ) {
        var baos = new ByteArrayOutputStream();
        puts.add( new Put( table, baos ) );

        return baos;
    }

    @Override
    public void deleteRows( String table, String where, boolean async, long timeout ) {
    }

    @Override
    public ClickhouseProcess putAsync( String table, DataFormat format, long timeout ) {
        throw new NotImplementedException( "" );
    }

    @Override
    public ClickhouseProcess putAsync( String table, DataFormat format, Collection<String> fields, long timeout ) {
        throw new NotImplementedException( "" );
    }

    @Override
    public long getTimeout() {
        return 0;
    }

    @Override
    public ClickhouseProcess executeAsync( String query, boolean useDatabase, long timeout ) {
        throw new NotImplementedException( "" );
    }

    @Override
    public void execute( String sql, boolean useDatabase, long timeout ) {
    }

    @Override
    public void createDatabase( long timeout ) {
        puts.add( new Put( null, null ) );
    }

    @Override
    public void dropDatabase( long timeout ) {
        drops.add( new Drop( null, null ) );
    }

    @Override
    public void dropTable( String table, long timeout ) {
        drops.add( new Drop( table, null ) );
    }

    @Override
    public int get( String query, Consumer<String> line, boolean useDatabase, long timeout ) throws ClickhouseException {
        getLines.forEach( line );

        return getLines.size();
    }

    @Override
    public Database getDatabase() {
        return new Database( database, this, new SystemSettings( this ) );
    }

    @Override
    public ClickhouseClient useDatabase( String database ) {
        return new MockClickhouseClient( database, drops, puts, execute );
    }

    @Override
    public ClickhouseClient withUser( String user ) {
        return this;
    }

    @ToString
    @EqualsAndHashCode
    @RequiredArgsConstructor
    public static class Drop {
        public final String table;
        public final String partition;
    }

    @ToString
    @EqualsAndHashCode
    @RequiredArgsConstructor
    public static class Put {
        public final String table;
        public final ByteArrayOutputStream content;
    }
}
