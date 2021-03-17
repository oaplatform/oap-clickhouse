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

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import oap.clickhouse.ClickhouseClient;
import oap.clickhouse.ConfigField;
import oap.clickhouse.Database;
import oap.clickhouse.FieldType;
import oap.clickhouse.TableInfo;
import oap.clickhouse.ViewInfo;
import oap.dictionary.Dictionary;
import oap.logstream.data.DataModel;
import oap.util.Lists;
import org.apache.commons.collections4.ListUtils;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;


@Slf4j
public class ClickhouseMigration {
    public final ClickhouseDatabaseConfiguration databaseConfiguration;
    public final long timeout;
    private final boolean addUploadTimeField;
    private final ClickhouseClient client;
    private final DataModel dataModel;
    public boolean dropDatabaseBeforeMigration;
    public Database db;
    public Path externalConfiguration = null;

    public ClickhouseMigration( ClickhouseClient client,
                                DataModel dataModel,
                                ClickhouseDatabaseConfiguration databaseConfiguration,
                                boolean dropDatabaseBeforeMigration,
                                boolean addUploadTimeField,
                                long timeout ) {
        this.client = client;
        this.dataModel = dataModel;
        this.databaseConfiguration = databaseConfiguration;
        this.dropDatabaseBeforeMigration = dropDatabaseBeforeMigration;
        this.addUploadTimeField = addUploadTimeField;
        this.timeout = timeout;
    }

    public void start() {
        log.info( "migration {}, addUploadTimeField {}, databaseConfiguration = {}",
            externalConfiguration, addUploadTimeField, databaseConfiguration );

        db = client.getDatabase();

        if( dropDatabaseBeforeMigration ) db.drop();

        db.createIfNotExists();

        var tableInfo = new ArrayList<TableInfo>();
        log.trace( "tables: {}", databaseConfiguration.tables.keySet() );
        for( var model : databaseConfiguration.tables.keySet() ) {
            var confTable = dataModel.model.getValue( model );
            if( confTable == null ) {
                log.warn( "{} model node not found", model );
                continue;
            }

            log.debug( "dictionary id = {}", confTable.getId() );


            tableInfo.addAll( getTableInfo( confTable ) );
        }

        var viewInfo = new ArrayList<ViewInfo>();
        databaseConfiguration.views.forEach( ( name, schema ) -> {
            log.debug( "view name = {}", name );

            viewInfo.addAll( getViewInfo( dataModel.model, name, schema ) );
        } );

        db.upgrade( tableInfo, viewInfo, timeout );
    }

    private List<ViewInfo> getViewInfo( Dictionary latestDictionary, String name, ViewSchema schema ) {
        var ret = new ArrayList<ViewInfo>();

        var source = latestDictionary.getValue( schema.fromTable );
        Preconditions.checkNotNull( source, "table " + schema.fromTable + " not found" );

        var fields = Lists.filterThanMap( source.getValues(),
            d -> d.getTags().contains( schema.tag ) && !Lists.contains( schema.aggregates, a -> d.getId().equals( a.field ) ),
            d -> new ViewInfo.Field( d.getId(), d.<String>getProperty( "alias" ).orElse( null ) )
        );

        if( addUploadTimeField ) {
            fields.add( 0, new ViewInfo.Field( "UPLOADTIME", null ) );
        }

        var tablePerThreads = databaseConfiguration.getTablePerThread( schema.fromTable.toLowerCase() );

        if( tablePerThreads > 1 ) {
            for( var i = 1; i <= tablePerThreads; i++ ) {
                ret.add( new ViewInfo( name + i, schema.materialized, schema.populate, fields, schema.aggregates, schema.where, schema.fromTable + i, schema.toTable ) );
            }
        } else {
            ret.add( new ViewInfo( name, schema.materialized, schema.populate, fields, schema.aggregates, schema.where, schema.fromTable, schema.toTable ) );
        }

        return ret;
    }

    private List<TableInfo> getTableInfo( Dictionary confTable ) {
        var ret = new ArrayList<TableInfo>();
        var tablePerThreads = databaseConfiguration.getTablePerThread( confTable.getId().toLowerCase() );
        var tableName = confTable.<String>getProperty( "table" ).orElse( confTable.getId() );

        if( tablePerThreads > 1 ) {
            for( var i = 1; i <= tablePerThreads; i++ ) {
                ret.add( getTableInfo( confTable, tableName, i ) );
            }
        } else {
            ret.add( getTableInfo( confTable, tableName, -1 ) );
        }

        return ret;
    }


    private TableInfo getTableInfo( Dictionary confTable, String tableName, int threadIdx ) {
        var fields = new ArrayList<ConfigField>();

        var tableEngine = databaseConfiguration.tables.get( tableName );
        Preconditions.checkNotNull( tableEngine, "table engine for " + tableName + " not found" );

        if( addUploadTimeField ) {
            var options = tableEngine.getOptions( "UPLOADTIME", databaseConfiguration.defaults.tables );
            fields.add( new ConfigField( "UPLOADTIME", FieldType.DATETIME, Optional.empty(), Optional.empty(), options.CODEC,
                Optional.empty(), Optional.empty(), Optional.empty(), 0 ) );
        }

        for( var d : confTable.getValues() ) {
            var typeStr = d.<String>getProperty( "type" ).orElse( "" );
            FieldType type;
            try {
                type = FieldType.valueOf( typeStr );
            } catch( IllegalArgumentException e ) {
                log.error( d.getId() + ":: Unknown field type: " + typeStr );
                throw e;
            }

            Optional<Number> length = d.getProperty( "length" );
            Optional<Boolean> lowCardinality = d.getProperty( "lowCardinality" );

            var option = tableEngine.getOptions( d.getId(), databaseConfiguration.defaults.tables );

            fields.add( new ConfigField(
                d.getId(),
                type,
                length,
                lowCardinality.or( () -> option.LowCardinality ),
                option.CODEC,
                d.getProperty( "dictionary" ),
                d.getProperty( "materialized" ),
                d.getProperty( "default" ),
                0
            ) );
        }

        var table = Preconditions.checkNotNull( databaseConfiguration.tables.get( tableName ) );
        var ttl = table.ttl;
        if( ttl != null ) {
            var fIndex = ListUtils.indexOf( fields, f -> f.name.equals( ttl.field ) );
            Preconditions.checkArgument( fIndex >= 0, " ttl field " + ttl + " not found" );
            fields.set( fIndex, fields.get( fIndex ).withTtl( ttl.getTtl() ) );
        }

        log.debug( "table = {}, idx = {}, fields = {}", tableName, threadIdx, fields );

        return new TableInfo( tableName + ( threadIdx > 0 ? String.valueOf( threadIdx ) : "" ),
            fields, tableEngine.indexes, tableEngine, Map.of( "THREAD_IDX", "" ) );
    }
}
