package oap.clickhouse.migration;

import oap.clickhouse.ClickHouseException;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;

/**
 * Created by igor.petrenko on 24.10.2016.
 */
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
    public boolean exists() throws ClickHouseException {
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

    public void upgrade( Supplier<String> init ) throws ClickHouseException {
        if( !exists() ) {
            log.debug( "Table {}.{} doesn't exist", database.getName(), name );
            database.client.execute( buildQuery( init.get(), emptyMap() ), true );
        } else {
            drop();
            upgrade( init );
        }
    }

    @Override
    public void drop() throws ClickHouseException {
        database.client.execute( buildQuery( DROP_DICTIONARY_SQL, emptyMap() ), true );
        refresh();
    }
}
