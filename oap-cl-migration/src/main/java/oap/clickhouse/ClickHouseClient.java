package oap.clickhouse;

import oap.clickhouse.migration.Database;

import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

/**
 * Created by igor.petrenko on 28.02.2018.
 */
public interface ClickHouseClient {
    default List<String> getLines( String query ) throws ClickHouseException {
        return getLines( query, true, getTimeout() );
    }

    default List<String> getLines( String query, long timeout ) throws ClickHouseException {
        return getLines( query, true, timeout );
    }

    long getTimeout();

    default List<String> getLines( String query, boolean useDatabase ) throws ClickHouseException {
        return getLines( query, useDatabase, getTimeout() );
    }

    List<String> getLines( String query, boolean useDatabase, long timeout ) throws ClickHouseException;


    default OutputStream put( String table, DataFormat format ) {
        return put( table, format, getTimeout() );
    }

    OutputStream put( String table, DataFormat format, long timeout );


    default void deleteRows( String table, String where, boolean async ) {
        deleteRows( table, where, async, getTimeout() );
    }

    void deleteRows( String table, String where, boolean async, long timeout );


    default ClickHouseProcess putAsync( String table, DataFormat format ) {
        return putAsync( table, format, getTimeout() );
    }

    ClickHouseProcess putAsync( String table, DataFormat format, long timeout );


    default ClickHouseProcess putAsync( String table, DataFormat format, Collection<String> fields ) {
        return putAsync( table, format, fields, getTimeout() );
    }

    ClickHouseProcess putAsync( String table, DataFormat format, Collection<String> fields, long timeout );


    default ClickHouseProcess executeAsync( String query, boolean useDatabase ) {
        return executeAsync( query, useDatabase, getTimeout() );
    }

    ClickHouseProcess executeAsync( String query, boolean useDatabase, long timeout );

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

    default int get( String query, Consumer<String> line ) throws ClickHouseException {
        return get( query, line, true, getTimeout() );
    }

    default int get( String query, Consumer<String> line, long timeout ) throws ClickHouseException {
        return get( query, line, true, timeout );
    }

    default int get( String query, Consumer<String> line, boolean useDatabase ) throws ClickHouseException {
        return get( query, line, useDatabase, getTimeout() );
    }

    int get( String query, Consumer<String> line, boolean useDatabase, long timeout ) throws ClickHouseException;

    Database getDatabase();

    ClickHouseClient useDatabase( String database );

    ClickHouseClient withUser( String user );

    default ClickHouseClient withDefaultUser() {
        return withUser( null );
    }

    interface ClickHouseProcess {
        ClickhouseStream toStream() throws ClickHouseException;
    }
}
