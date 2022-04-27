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

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import lombok.AllArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import oap.tsv.Tsv;
import oap.util.Lists;
import oap.util.Strings;
import org.apache.commons.lang3.StringUtils;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;

@Slf4j
public class AbstractTable {
    public static final String OPTIMIZE_TABLE_SQL = "OPTIMIZE TABLE ${DATABASE}.${TABLE} ${FINAL}";

    private static final String TABLE_EXISTS_SQL =
        "SELECT name, engine, partition_key, primary_key, dependencies_table, create_table_query FROM system.tables "
            + "WHERE database = '${DATABASE}' AND name = '${TABLE}' FORMAT TabSeparatedRaw";

    private static final String DROP_TABLE_SQL = "DROP TABLE IF EXISTS ${DATABASE}.${TABLE}";

    private static final String FIELDS_QUERY =
        "SELECT name, type, default_kind, default_expression, compression_codec, "
            + "is_in_partition_key, is_in_sorting_key, is_in_primary_key, is_in_sampling_key "
            + "FROM system.columns "
            + "WHERE database = '${DATABASE}' AND table = '${TABLE}' FORMAT TabSeparatedRaw";
    protected final Cache<String, Object> cache = CacheBuilder
        .newBuilder()
        .expireAfterWrite( Duration.ofMinutes( 1 ) )
        .build();
    protected final String name;
    protected Database database;

    protected AbstractTable( Database database, String name ) {
        this.database = database;
        this.name = name;
    }

    protected String buildQuery( String query, Map<String, String> params ) {
        return Strings.substitute( query, field ->
            switch( field ) {
                case "DATABASE" -> database.getName();
                case "TABLE" -> name;
                case "TABLE_SUFFIX" -> StringUtils.stripToEmpty( System.getProperty( "TABLE_SUFFIX" ) );
                default -> params.get( field );
            } );
    }

    public void drop() throws ClickhouseException {
        database.client.execute( buildQuery( DROP_TABLE_SQL, emptyMap() ), true );
        refresh();
    }

    public boolean exists() throws ClickhouseException {
        try {
            return ( Boolean ) cache.get( "exists", () -> {
                var sql = buildQuery( TABLE_EXISTS_SQL, emptyMap() );
                log.trace( "sql = {}", sql );
                var lines = database.client.getLines( sql, false );
                return !lines.isEmpty();
            } );
        } catch( ExecutionException e ) {
            throw getException( e );
        }
    }

    protected ClickhouseException getException( ExecutionException e ) {
        if( e.getCause() instanceof ClickhouseException ) return ( ClickhouseException ) e.getCause();
        if( e.getCause() != null ) return new ClickhouseException( e.getCause() );
        return new ClickhouseException( e );
    }

    public void refresh() {
        cache.invalidateAll();
        cache.cleanUp();
    }

    @SuppressWarnings( { "unchecked", "checkstyle:LocalVariableName" } )
    public LinkedHashMap<String, FieldInfo> getFields() throws ClickhouseException {
        try {
            return ( LinkedHashMap<String, FieldInfo> ) cache.get( "getFields", () -> {
                var fields = new LinkedHashMap<String, FieldInfo>();

                var sql = buildQuery( FIELDS_QUERY, Map.of() );
                log.trace( "sql = {}", sql );
                var lines = database.client.getLines( sql, false );

                log.trace( "lines = {}", lines );

                for( String line : lines ) {
                    var cols = StringUtils.splitPreserveAllTokens( line, '\t' );

                    var name = cols[0];
                    var type = cols[1];
                    var default_type = cols[2];
                    var default_expression = cols[3];
                    var compression_codec = cols[4];
                    var is_in_partition_key = "1".equals( cols[5] );
                    var is_in_sorting_key = "1".equals( cols[6] );
                    var is_in_primary_key = "1".equals( cols[7] );
                    var is_in_sampling_key = "1".equals( cols[8] );

                    fields.put( name, new FieldInfo( name, type, default_type, default_expression,
                        compression_codec,
                        is_in_partition_key, is_in_sorting_key,
                        is_in_primary_key, is_in_sampling_key ) );
                }

                log.trace( "fields = {}", fields );

                return fields;
            } );
        } catch( ExecutionException e ) {
            throw getException( e );
        }
    }

    public void optimize() {
        optimize( false );
    }

    public void optimize( boolean isFinal ) {
        database.client.execute( buildQuery( OPTIMIZE_TABLE_SQL, Map.of( "FINAL", isFinal ? "FINAL" : "" ) ), false );
    }

    public Info getInfo() {
        try {
            return ( Info ) cache.get( "getInfo", () -> {
                var sql = buildQuery( TABLE_EXISTS_SQL, emptyMap() );
                log.trace( "sql = {}", sql );
                var lines = database.client.getLines( sql, false );
                Preconditions.checkNotNull( lines );
                Preconditions.checkArgument( lines.size() == 1 );
                var list = Tsv.tsv.parse( lines.get( 0 ) );
                Preconditions.checkArgument( list.size() == 6 );

                var engine = Engine.valueOf( list.get( 1 ) );
                var primaryKeyTsv = list.get( 3 );
                var dependenciesTable = list.get( 4 );
                dependenciesTable = dependenciesTable.substring( 1, dependenciesTable.length() - 1 );

                return new Info( list.get( 0 ), engine, list.get( 2 ),
                    Lists.map( asList( StringUtils.split( primaryKeyTsv, ',' ) ), String::trim ),
                    Lists.map( asList( StringUtils.split( dependenciesTable, ',' ) ), name ->
                        name.substring( 1, name.length() - 1 ) ),
                    list.get( 5 ) );
            } );
        } catch( ExecutionException e ) {
            throw getException( e );
        }
    }

    @ToString
    @AllArgsConstructor
    public static class Info {
        public final String name;
        public final Engine engine;
        public final String partitionKey;
        public final List<String> primaryKey;
        public final List<String> dependenciesTable;
        public final String createTableQuery;
    }

    @ToString
    @AllArgsConstructor
    @SuppressWarnings( "checkstyle:MemberName" )
    public static class FieldInfo {
        public final String name;
        public final String type;
        public final String default_kind;
        public final String default_expression;
        public final String compression_codec;
        public final boolean is_in_partition_key;
        public final boolean is_in_sorting_key;
        public final boolean is_in_primary_key;
        public final boolean is_in_sampling_key;

        public boolean isMaterialized() {
            return "MATERIALIZED".equalsIgnoreCase( default_kind );
        }

        public boolean isArray() {
            return type.startsWith( "Array(" );
        }

        public String getDropSql() {
            return "ALTER TABLE ${DATABASE}.${TABLE} DROP COLUMN " + name;
        }
    }
}
