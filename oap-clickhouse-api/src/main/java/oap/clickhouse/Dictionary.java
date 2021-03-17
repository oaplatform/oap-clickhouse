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
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;

@ToString( callSuper = true )
@EqualsAndHashCode( callSuper = true )
@Slf4j
public class Dictionary extends AbstractTable {
    private static final String DICTIONARY_EXISTS_QUERY =
        "SELECT name FROM system.tables WHERE database = '${DATABASE}' AND name = '${TABLE}'";

    private static final String DROP_DICTIONARY_SQL =
        "DROP TABLE IF EXISTS ${DATABASE}.${TABLE}";


    public Dictionary( Database database, String name ) {
        super( database, name );
    }


    @Override
    public boolean exists() throws ClickhouseException {
        try {
            return ( Boolean ) cache.get( "exists", () -> {
                var sql = buildQuery( DICTIONARY_EXISTS_QUERY, emptyMap() );
                log.trace( "sql = {}", sql );
                final List<String> lines = database.client.getLines( sql );
                return !lines.isEmpty();
            } );
        } catch( ExecutionException e ) {
            throw getException( e );
        }
    }

    public void upgrade( Supplier<String> init ) throws ClickhouseException {
        if( !exists() ) {
            log.debug( "Table {}.{} doesn't exist", database.getName(), name );
            database.client.execute( buildQuery( init.get(), emptyMap() ), true );
        } else {
            drop();
            upgrade( init );
        }
    }

    @Override
    public void drop() throws ClickhouseException {
        database.client.execute( buildQuery( DROP_DICTIONARY_SQL, emptyMap() ), true );
        refresh();
    }
}
