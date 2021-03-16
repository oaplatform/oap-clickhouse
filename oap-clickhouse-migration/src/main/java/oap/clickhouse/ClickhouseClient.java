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

import oap.clickhouse.migration.Database;

import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

public interface ClickhouseClient {
    default List<String> getLines( String query ) throws ClickhouseException {
        return getLines( query, true, getTimeout() );
    }

    default List<String> getLines( String query, long timeout ) throws ClickhouseException {
        return getLines( query, true, timeout );
    }

    long getTimeout();

    default List<String> getLines( String query, boolean useDatabase ) throws ClickhouseException {
        return getLines( query, useDatabase, getTimeout() );
    }

    List<String> getLines( String query, boolean useDatabase, long timeout ) throws ClickhouseException;


    default OutputStream put( String table, DataFormat format ) {
        return put( table, format, getTimeout() );
    }

    OutputStream put( String table, DataFormat format, long timeout );

    OutputStream put( String table, DataFormat format, Collection<String> fields, long timeout );


    default void deleteRows( String table, String where, boolean async ) {
        deleteRows( table, where, async, getTimeout() );
    }

    void deleteRows( String table, String where, boolean async, long timeout );


    default ClickhouseProcess putAsync( String table, DataFormat format ) {
        return putAsync( table, format, getTimeout() );
    }

    ClickhouseProcess putAsync( String table, DataFormat format, long timeout );


    default ClickhouseProcess putAsync( String table, DataFormat format, Collection<String> fields ) {
        return putAsync( table, format, fields, getTimeout() );
    }

    ClickhouseProcess putAsync( String table, DataFormat format, Collection<String> fields, long timeout );


    default ClickhouseProcess executeAsync( String query, boolean useDatabase ) {
        return executeAsync( query, useDatabase, getTimeout() );
    }

    ClickhouseProcess executeAsync( String query, boolean useDatabase, long timeout );

    default void execute( String sql, boolean useDatabase ) {
        execute( sql, useDatabase, getTimeout() );
    }

    void execute( String sql, boolean useDatabase, long timeout );

    default void createDatabase() {
        createDatabase( getTimeout() );
    }

    void createDatabase( long timeout );

    default void dropDatabase() {
        dropDatabase( getTimeout() );
    }

    void dropDatabase( long timeout );

    void dropTable( String table, long timeout );

    default void dropTable( String table ) {
        dropTable( table, getTimeout() );
    }

    default int get( String query, Consumer<String> line ) throws ClickhouseException {
        return get( query, line, true, getTimeout() );
    }

    default int get( String query, Consumer<String> line, long timeout ) throws ClickhouseException {
        return get( query, line, true, timeout );
    }

    default int get( String query, Consumer<String> line, boolean useDatabase ) throws ClickhouseException {
        return get( query, line, useDatabase, getTimeout() );
    }

    int get( String query, Consumer<String> line, boolean useDatabase, long timeout ) throws ClickhouseException;

    Database getDatabase();

    ClickhouseClient useDatabase( String database );

    ClickhouseClient withUser( String user );

    default ClickhouseClient withDefaultUser() {
        return withUser( null );
    }

    interface ClickhouseProcess {
        ClickhouseStream toStream() throws ClickhouseException;
    }
}
