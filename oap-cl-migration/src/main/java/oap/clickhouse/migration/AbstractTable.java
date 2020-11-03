package oap.clickhouse.migration;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import oap.clickhouse.ClickHouseException;
import lombok.AllArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import oap.logstream.tsv.Tsv;
import oap.util.Lists;
import oap.util.Strings;
import org.apache.commons.lang3.StringUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;

/**
 * Created by igor.petrenko on 03.04.2018.
 */
@Slf4j
public class AbstractTable {
    public static final String OPTIMIZE_TABLE_SQL = "OPTIMIZE TABLE ${DATABASE}.${TABLE} ${FINAL}";

    private static final String TABLE_EXISTS_SQL =
        "SELECT name, engine, partition_key, primary_key, dependencies_table, create_table_query FROM system.tables "
            + "WHERE database = '${DATABASE}' AND name = '${TABLE}' FORMAT TabSeparatedRaw";

    private static final String DROP_TABLE_SQL = "DROP TABLE IF EXISTS ${DATABASE}.${TABLE}";

    private static final String FIELDS_QUERY =
        "SELECT name, type, default_kind, default_expression, is_in_partition_key, is_in_sorting_key, is_in_primary_key, is_in_sampling_key "
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
        return Strings.substitute( query, ( field ) -> {
            switch( field ) {
                case "DATABASE":
                    return database.getName();
                case "TABLE":
                    return name;
                case "TABLE_SUFFIX":
                    return StringUtils.stripToEmpty( System.getProperty( "TABLE_SUFFIX" ) );
                default:
                    return params.get( field );
            }
        } );
    }

    public void drop() throws ClickHouseException {
        database.client.execute( buildQuery( DROP_TABLE_SQL, emptyMap() ), true );
        refresh();
    }

    public boolean exists() throws ClickHouseException {
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

    protected ClickHouseException getException( ExecutionException e ) {
        if( e.getCause() instanceof ClickHouseException ) return ( ClickHouseException ) e.getCause();
        return new ClickHouseException( e.getCause() );
    }

    public void refresh() {
        cache.invalidateAll();
        cache.cleanUp();
    }

    @SuppressWarnings( "unchecked" )
    public Map<String, FieldInfo> getFields() throws ClickHouseException {
        try {
            return ( Map<String, FieldInfo> ) cache.get( "getFields", () -> {
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
                    var is_in_partition_key = "1".equals( cols[4] );
                    var is_in_sorting_key = "1".equals( cols[5] );
                    var is_in_primary_key = "1".equals( cols[6] );
                    var is_in_sampling_key = "1".equals( cols[7] );

                    fields.put( name, new FieldInfo( name, type, default_type, default_expression, is_in_partition_key, is_in_sorting_key,
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
                Preconditions.checkArgument( lines.size() == 1 );
                var list = new ArrayList<String>();
                Tsv.split( lines.get( 0 ), list );
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
    public static class FieldInfo {
        public final String name;
        public final String type;
        public final String default_kind;
        public final String default_expression;
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
