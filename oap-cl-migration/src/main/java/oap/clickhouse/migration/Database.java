package oap.clickhouse.migration;

import com.google.common.base.Preconditions;
import oap.clickhouse.ClickHouseClient;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import oap.util.Lists;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by igor.petrenko on 24.10.2016.
 */
@ToString
@EqualsAndHashCode
@Slf4j
public class Database {
    private static ConcurrentHashMap<String, Table> tables = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, View> views = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, Dictionary> dictionaries = new ConcurrentHashMap<>();

    public final ClickHouseClient client;
    public final String database;

    public Database( String database, ClickHouseClient client ) {
        this.database = database;
        this.client = client;
    }

    public Table getTable( String name ) {
        return tables.computeIfAbsent( name, n -> new Table( this, n ) );
    }

    public View getView( String name ) {
        return views.computeIfAbsent( name, v -> new View( this, v ) );
    }

    public Dictionary getDictionary( String name ) {
        return dictionaries.computeIfAbsent( name, d -> new Dictionary( this, d ) );
    }

    public void createIfNotExists() {
        client.createDatabase();
    }

    public void drop() {
        client.dropDatabase();
        refresh();
    }

    public String getName() {
        return database;
    }

    public void upgrade( List<TableInfo> tables, List<ViewInfo> views, long timeout ) {
        for( var v : views ) {
            var ti = Lists.find2( tables, t -> t.name.equals( v.fromTable ) );
            Preconditions.checkNotNull( ti, "table " + v.fromTable + " not found" );
            var changed = getTable( ti.name ).upgrade( ti.fields, ti.indexes, ti.tableEngine, ti.params, true, timeout );
            var view = getView( v.name );
            if( changed ) {
                if( view.exists() ) view.drop();
            } else {
                var fields = view.getFields();
                if( !v.equalFields( fields.keySet(), v.aggregates ) ) {
                    view.drop();
                }
            }
        }

        for( var table : tables ) {
            getTable( table.name ).upgrade( table.fields, table.indexes, table.tableEngine, table.params, false, timeout );
        }

        for( var v : views ) {
            var view = getView( v.name );

            var fromTable = getTable( v.fromTable );
            var info = fromTable.getInfo();
            log.trace( "table info {}", info );

            var pkeys =
                v.pk.isEmpty()
                    ? Lists.filter( info.primaryKey, fc -> Lists.find2( v.getAllFields(), f -> f.equals( fc ) ) != null )
                    : v.pk;

            String engine = null;
            if(v.toTable.isEmpty()) {
                var engineName = info.engine;
                engine = engineName + "() PARTITION BY " + info.partitionKey + " ORDER BY (" + String.join( ",", pkeys ) + ")";
            }

            if( !view.exists() ) {
                view.create( v, engine );
            }
        }

        refresh();
    }

    public void refresh() {
        tables.clear();
        views.clear();
        dictionaries.clear();
    }
}
