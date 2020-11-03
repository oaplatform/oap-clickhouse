package oap.clickhouse.migration;

import oap.clickhouse.ClickHouseException;
import oap.clickhouse.migration.FieldType.LowCardinality;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import oap.util.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import static oap.clickhouse.migration.SqlUtils.addFieldsIndexesToInitQuery;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toMap;

/**
 * Created by igor.petrenko on 24.10.2016.
 */
@ToString( callSuper = true )
@EqualsAndHashCode( callSuper = true )
@Slf4j
public class Table extends AbstractTable {
    private static final String CREATE_TABLE_QUERY =
        "SELECT create_table_query FROM system.tables WHERE database = '${DATABASE}' AND name = '${TABLE}' FORMAT TabSeparated";

    private static final String TRUNCATE_TABLE_SQL = "TRUNCATE TABLE IF EXISTS ${DATABASE}.${TABLE}";

    public Table( Database database, String name ) {
        super( database, name );
    }

    private static ConfigField getTtlField( List<ConfigField> fields ) {
        return Lists.find2( fields, f -> f.ttl > 0 );
    }

    public TtlInfo getTtlField() throws ClickHouseException {
        try {
            var getTtlField = ( TtlInfo ) cache.get( "getTtlField", () -> {
                var sql = buildQuery( CREATE_TABLE_QUERY, emptyMap() );
                log.trace( "sql = {}", sql );
                var lines = database.client.getLines( sql );
                if( lines.isEmpty() ) return null;

                var TTL_PATTERN = Pattern.compile( "\\sTTL\\s([^\\s]+)\\s\\+\\s[^(]+\\((\\d+)\\)" );
                var matcher = TTL_PATTERN.matcher( lines.get( 0 ) );
                if( matcher.find() ) return new TtlInfo( matcher.group( 1 ), Integer.parseInt( matcher.group( 2 ) ) );

                return TtlInfo.NULL;
            } );
            return getTtlField == TtlInfo.NULL ? null : getTtlField;
        } catch( ExecutionException e ) {
            throw getException( e );
        }
    }

    public int getIndexGranularity() throws ClickHouseException {
        try {
            return ( Integer ) cache.get( "getIndexGranularity", () -> {
                var sql = buildQuery( CREATE_TABLE_QUERY, emptyMap() );
                log.trace( "sql = {}", sql );
                var lines = database.client.getLines( sql );
                if( lines.isEmpty() ) return -1;

                var index_granularity_PATTERN = Pattern.compile( "index_granularity\\s*=\\s*(\\d+)" );
                var matcher = index_granularity_PATTERN.matcher( lines.get( 0 ) );
                if( matcher.find() ) return Integer.parseInt( matcher.group( 1 ) );

                return -1;
            } );
        } catch( ExecutionException e ) {
            throw getException( e );
        }
    }

    boolean upgrade( List<ConfigField> fields,
                     List<ConfigIndex> indexes,
                     TableEngine tableEngine,
                     Map<String, String> params,
                     boolean dryRun,
                     long timeout ) {
        var exists = exists();
        if( exists && isMemoryEngine() ) {
            exists = false;
            if( !dryRun )
                drop();
        }
        if( !exists ) {
            if( !dryRun ) {
                log.debug( "Table {}.{} doesn't exist", database.getName(), name );
                var createTableQuery = addFieldsIndexesToInitQuery( tableEngine, fields, indexes ).trim();

                if( tableEngine.engine.supportTtl ) {
                    var ttlField = getTtlField( fields );
                    if( ttlField != null ) {
                        if( createTableQuery.endsWith( ";" ) )
                            createTableQuery = createTableQuery.substring( 0, createTableQuery.length() - 1 );
                        createTableQuery += "\nTTL " + ttlField.name + " + toIntervalSecond(" + ttlField.ttl + ")";
                    }
                }

                database.client.execute( buildQuery( createTableQuery, params ), true, timeout );
            }
            refresh();
            return true;
        } else {
            var tableFields = getFields();
            var mapConfigFields = fields.stream().collect( toMap( cf -> cf.name, cf -> cf ) );

            var modified = false;

            ConfigField prev = null;

            for( var cf : fields ) {
                if( !tableFields.containsKey( cf.name ) ) {
                    if( prev == null )
                        throw new ClickHouseException( "no way to add a column " + cf.name + " to the beginning of a table " + database.getName() + "." + name, 0, null );

                    log.debug( "add field {} after {}", cf.name, prev.name );
                    if( !dryRun ) {
                        database.client.execute( buildQuery( cf.getAddSql(), Map.of( "AFTER", prev.name ) ), true, timeout );
                        refresh();
                    }
                    modified = true;
                }
                prev = cf;
            }

            for( var tf : tableFields.values() ) {
                if( !mapConfigFields.containsKey( tf.name ) ) {
                    log.debug( "dropPartition field {}", tf.name );

                    if( !dryRun ) {
                        database.client.execute( buildQuery( tf.getDropSql(), emptyMap() ), true, timeout );
                        refresh();
                    }
                    modified = true;
                }
            }

            for( var cf : fields ) {
                var tableField = tableFields.get( cf.name );
                if( tableField != null ) {
                    if( !cf.typeEquals( tableField.type ) ) {
                        log.trace( "modify field {}, type: {} -> {}", cf.name, tableField.type, cf.type.toClickHouseType( cf.length, cf.enumName, cf.lowCardinality.filter( lc -> lc ).map( lc -> LowCardinality.ON ).orElse( LowCardinality.OFF ) ) );
                        if( !dryRun )
                            database.client.execute( buildQuery( cf.getModifySql(), emptyMap() ), true, timeout );
                        modified = true;
                    }
                }
            }

            if( !isMemoryEngine() ) {
                var ttlField = getTtlField( fields );
                var currentTableTtlField = getTtlField();
                if( ttlField != null ) {
                    if( currentTableTtlField == null || !currentTableTtlField.name.equals( ttlField.name ) || currentTableTtlField.ttl != ttlField.ttl ) {
                        if( !dryRun ) {
                            database.client.execute( buildQuery( "ALTER TABLE ${DATABASE}.${TABLE} MODIFY TTL " + ttlField.name + " + INTERVAL " + ttlField.ttl + " SECOND", emptyMap() ), true, timeout );
                            refresh();
                        }
                        modified = true;
                    }
                } else {
                    var tableTtlField = getTtlField();
                    if( tableTtlField != null ) {
                        if( !dryRun )
                            database.client.execute( buildQuery( "ALTER TABLE ${DATABASE}.${TABLE} MODIFY TTL " + tableTtlField.name, emptyMap() ), true, timeout );
                        modified = true;
                    }
                }

                var tableIndexes = getIndexes();
                for( var index : tableIndexes ) {
                    var found = Lists.find2( indexes, in -> in.name.equals( index.name ) );
                    if( index.equals( found ) ) continue;

                    if( !dryRun ) {
                        database.client.execute( buildQuery( "ALTER TABLE ${DATABASE}.${TABLE} DROP INDEX " + index.name, emptyMap() ), true, timeout );
                        refresh();
                    }
                    modified = true;
                }

                tableIndexes = getIndexes();
                for( var index : indexes ) {
                    if( Lists.find2( tableIndexes, ti -> ti.name.equals( index.name ) ) != null ) continue;

                    if( !dryRun )
                        database.client.execute( buildQuery( "ALTER TABLE ${DATABASE}.${TABLE} ADD " + index.getIndexSql(), emptyMap() ), true, timeout );
                    modified = true;
                }
            }

            if( modified ) refresh();
            return modified;

        }

    }

    public boolean isMemoryEngine() throws ClickHouseException {
        try {
            return ( Boolean ) cache.get( "isMemoryEngine", () -> {
                var sql = buildQuery( CREATE_TABLE_QUERY, emptyMap() );
                log.trace( "sql = {}", sql );
                var lines = database.client.getLines( sql );
                if( lines.size() != 1 ) {
                    return false;
                }

                var memoryEngine =
                    Pattern.compile( "\\s*Engine\\s*=\\s*memory", Pattern.CASE_INSENSITIVE ).matcher( lines.get( 0 ) ).find()
                        ? 1 : 0;

                return memoryEngine == 1;
            } );
        } catch( ExecutionException e ) {
            throw getException( e );
        }
    }

    public void truncate() throws ClickHouseException {
        database.client.execute( buildQuery( TRUNCATE_TABLE_SQL, emptyMap() ), true );
    }

    @SuppressWarnings( "unchecked" )
    public List<ConfigIndex> getIndexes() throws ClickHouseException {
        try {
            return ( List<ConfigIndex> ) cache.get( "getIndexes", () -> {
                var sql = buildQuery( CREATE_TABLE_QUERY, emptyMap() );
                log.trace( "sql = {}", sql );
                var lines = database.client.getLines( sql );
                if( lines.isEmpty() ) return null;

                var res = new ArrayList<ConfigIndex>();

                var INDEX_PATTERN = Pattern.compile( "INDEX\\s+([^\\s(]+)\\s+\\(?(.+?(?=\\)?\\s*TYPE))\\)?\\s*TYPE\\s+([^\\s)]+\\)?)\\s*GRANULARITY\\s+(\\d+)" );
                var matcher = INDEX_PATTERN.matcher( lines.get( 0 ) );
                while( matcher.find() ) {
                    var name = matcher.group( 1 );
                    var fieldsStr = matcher.group( 2 );
                    var fields = List.of( fieldsStr.trim().split( "\\s*,\\s*" ) );
                    var type = matcher.group( 3 );
                    var granularity = matcher.group( 4 );

                    res.add( ConfigIndex.index( name, fields, type, Integer.parseInt( granularity ) ) );
                }
//        if( matcher.find() ) return new TtlInfo( matcher.group( 1 ), Integer.parseInt( matcher.group( 2 ) ) );

                return res;
            } );
        } catch( ExecutionException e ) {
            throw getException( e );
        }
    }

    @ToString
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class TtlInfo {
        public static final TtlInfo NULL = new TtlInfo( null, -1 );

        public final String name;
        public final int ttl;
    }

}
