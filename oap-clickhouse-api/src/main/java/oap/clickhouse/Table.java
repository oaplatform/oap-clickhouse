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
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import oap.clickhouse.FieldType.LowCardinality;
import oap.util.Lists;

import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import static java.util.Collections.emptyMap;
import static oap.clickhouse.SqlUtils.addFieldsIndexesToInitQuery;
import static oap.util.Collectors.toLinkedHashMap;

@ToString( callSuper = true )
@EqualsAndHashCode( callSuper = true )
@Slf4j
public class Table extends AbstractTable {
    public static final Pattern INDEX_GRANULARITY_PATTERN = Pattern.compile( "index_granularity\\s*=\\s*(\\d+)" );
    private static final String CREATE_TABLE_QUERY =
        "SELECT create_table_query FROM system.tables WHERE database = '${DATABASE}' AND name = '${TABLE}' FORMAT TabSeparated";
    private static final String TRUNCATE_TABLE_SQL = "TRUNCATE TABLE IF EXISTS ${DATABASE}.${TABLE}";
    private static final Pattern TTL_PATTERN = Pattern.compile( "\\sTTL\\s([^\\s]+)\\s\\+\\s[^(]+\\((\\d+)\\)" );
    private static final Pattern INDEX_PATTERN = Pattern.compile( "INDEX\\s+([^\\s(]+)\\s+\\(?(.+?(?=\\)?\\s*TYPE))\\)?\\s*TYPE\\s+([^\\s)]+\\)?)\\s*GRANULARITY\\s+(\\d+)" );

    public Table( Database database, String name ) {
        super( database, name );
    }

    private static ConfigField getTtlField( List<ConfigField> fields ) {
        return Lists.find2( fields, f -> f.ttl > 0 );
    }

    public TtlInfo getTtlField() throws ClickhouseException {
        try {
            var getTtlField = ( TtlInfo ) cache.get( "getTtlField", () -> {
                var createTableSql = getCreateTableSql();

                var matcher = TTL_PATTERN.matcher( createTableSql );
                if( matcher.find() ) return new TtlInfo( matcher.group( 1 ), Integer.parseInt( matcher.group( 2 ) ) );

                return TtlInfo.NULL;
            } );
            return getTtlField == TtlInfo.NULL ? null : getTtlField;
        } catch( ExecutionException e ) {
            throw getException( e );
        }
    }

    // TODO Lists.moveItem
    private static <E> void moveItem( List<E> list, int sourceIndex, int targetIndex ) {
        if( sourceIndex <= targetIndex ) {
            Collections.rotate( list.subList( sourceIndex, targetIndex + 1 ), -1 );
        } else {
            Collections.rotate( list.subList( targetIndex, sourceIndex + 1 ), 1 );
        }
    }

    private String getCreateTableSql() throws ExecutionException {
        var createTableSql = ( String ) cache.get( "createTableSql", () -> {
            var sql = buildQuery( CREATE_TABLE_QUERY, emptyMap() );
            log.trace( "sql = {}", sql );
            var lines = database.client.getLines( sql );
            if( lines.isEmpty() ) return "";
            return lines.get( 0 );
        } );
        return createTableSql;
    }

    public int getIndexGranularity() throws ClickhouseException {
        try {
            return ( Integer ) cache.get( "getIndexGranularity", () -> {
                var createTableSql = getCreateTableSql();

                var matcher = INDEX_GRANULARITY_PATTERN.matcher( createTableSql );
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
            var modified = addFields( fields, dryRun, timeout );
            modified |= dropFields( fields, dryRun, timeout );
            modified |= modifyFields( fields, dryRun, timeout );
            modified |= reorderFields( fields, dryRun, timeout );

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
                log.debug( "indexes = {}", tableIndexes );

                for( var index : tableIndexes ) {
                    var found = Lists.find2( indexes, in -> in.name.equals( index.name ) );
                    if( index.equals( found ) ) continue;

                    if( !dryRun ) {
                        database.client.execute( buildQuery( "ALTER TABLE ${DATABASE}.${TABLE} DROP INDEX " + index.name, emptyMap() ), true, timeout );
                        refresh();
                    }
                    modified = true;
                }

                refresh();
                tableIndexes = getIndexes();
                log.debug( "indexes = {}", tableIndexes );
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

    private boolean reorderFields( List<ConfigField> fields, boolean dryRun, long timeout ) {
        if( dryRun ) return false;

        var modified = false;
        var tableFields = getFields();
        var tableFieldsOrdered = new ArrayList<>( tableFields.keySet() );

        for( var idx = 0; idx < fields.size(); idx++ ) {
            var idxField = fields.get( idx );
            var tableIndex = tableFieldsOrdered.indexOf( idxField.name );
            Preconditions.checkArgument( tableIndex >= 0, "Field '" + idxField.name + "' not found, available: " + tableFieldsOrdered );

            if( tableIndex != idx ) {
                if( idx == 0 ) {
                    log.trace( "move fields {} -> FIRST", idxField.name );

                    checkModified( tableFields.get( idxField.name ) );

                    if( !dryRun )
                        database.client.execute( buildQuery( idxField.getModifySql(), Map.of( "AFTER_OR_FIRST", "FIRST" ) ), true, timeout );
                } else {
                    var afterField = tableFieldsOrdered.get( idx - 1 );
                    log.trace( "move fields {} -> AFTER {}", idxField.name, afterField );

                    checkModified( tableFields.get( idxField.name ) );

                    if( !dryRun )
                        database.client.execute( buildQuery( idxField.getModifySql(), Map.of( "AFTER_OR_FIRST", "AFTER " + afterField ) ), true, timeout );
                }


                moveItem( tableFieldsOrdered, tableIndex, idx );
                log.trace( "tableFieldsOrdered = {}}", tableFieldsOrdered );

                modified = true;
            }
        }
        return modified;
    }

    private boolean modifyFields( List<ConfigField> fields, boolean dryRun, long timeout ) {
        var modified = false;

        var tableFields = getFields();
        for( var cf : fields ) {
            var tableField = tableFields.get( cf.name );
            if( tableField != null ) {
                if( !cf.typeEquals( tableField.type, tableField.compression_codec ) ) {
                    log.trace( "modify field {}, type: {} -> {}, codec: {} -> {}",
                        cf.name,
                        tableField.type, cf.type.toClickhouseType( cf.length, cf.enumName, cf.lowCardinality.filter( lc -> lc ).map( lc -> LowCardinality.ON ).orElse( LowCardinality.OFF ) ),
                        cf.codec, tableField.compression_codec );

                    checkModified( tableField );
                    if( !dryRun )
                        database.client.execute( buildQuery( cf.getModifySql(), emptyMap() ), true, timeout );
                    modified = true;
                }
            }
        }
        return modified;
    }

    private void checkModified( FieldInfo tableField ) {
        if( database.settings.isPreventModify() ) {
            throw new ClickhouseException( "field '" + tableField.name + "' cannot be modified", HttpURLConnection.HTTP_FORBIDDEN, "settings prevent_modify has set" );
        }
    }

    private boolean dropFields( List<ConfigField> fields, boolean dryRun, long timeout ) {
        var modified = false;

        var tableFields = getFields();
        var mapConfigFields = fields.stream().collect( toLinkedHashMap( cf -> cf.name, cf -> cf ) );

        for( var tf : new ArrayList<>( tableFields.values() ) ) {
            if( !mapConfigFields.containsKey( tf.name ) ) {
                log.debug( "drop field {}", tf.name );

                if( database.settings.isPreventDestroy() ) {
                    throw new ClickhouseException( "field '" + tf.name + "' cannot be removed", HttpURLConnection.HTTP_FORBIDDEN, "settings prevent_destroy has set" );
                }

                if( !dryRun ) {
                    database.client.execute( buildQuery( tf.getDropSql(), emptyMap() ), true, timeout );
                    tableFields.remove( tf.name );
                }
                modified = true;
            }
        }
        return modified;
    }

    private boolean addFields( List<ConfigField> fields, boolean dryRun, long timeout ) {
        ConfigField prev = null;
        var modified = false;
        for( var cf : fields ) {
            if( !getFields().containsKey( cf.name ) ) {
                var order = prev != null ? " AFTER " + prev.name : "FIRST";
                log.debug( "add field {} {}", cf.name, order );
                if( !dryRun ) {
                    database.client.execute( buildQuery( cf.getAddSql(), Map.of( "AFTER_OR_FIRST", order ) ), true, timeout );
                    refresh();
                }
                modified = true;
            }
            prev = cf;
        }
        return modified;
    }

    public boolean isMemoryEngine() throws ClickhouseException {
        try {
            return ( Boolean ) cache.get( "isMemoryEngine", () -> {
                var createTableSql = getCreateTableSql();

                var memoryEngine =
                    Pattern.compile( "\\s*Engine\\s*=\\s*memory", Pattern.CASE_INSENSITIVE ).matcher( createTableSql ).find()
                        ? 1 : 0;

                return memoryEngine == 1;
            } );
        } catch( ExecutionException e ) {
            throw getException( e );
        }
    }

    public void truncate() throws ClickhouseException {
        database.client.execute( buildQuery( TRUNCATE_TABLE_SQL, emptyMap() ), true );
    }

    @SuppressWarnings( "unchecked" )
    public List<ConfigIndex> getIndexes() throws ClickhouseException {
        try {
            return ( List<ConfigIndex> ) cache.get( "getIndexes", () -> {
                var createTableSql = getCreateTableSql();

                var res = new ArrayList<ConfigIndex>();

                var matcher = INDEX_PATTERN.matcher( createTableSql );
                while( matcher.find() ) {
                    var name = matcher.group( 1 );
                    var fieldsStr = matcher.group( 2 );
                    var fields = List.of( fieldsStr.trim().split( "\\s*,\\s*" ) );
                    var type = matcher.group( 3 );
                    var granularity = matcher.group( 4 );

                    res.add( ConfigIndex.index( name, fields, type, Integer.parseInt( granularity ) ) );
                }

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
