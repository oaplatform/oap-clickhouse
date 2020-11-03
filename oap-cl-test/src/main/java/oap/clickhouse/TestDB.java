package oap.clickhouse;

import oap.testng.Teamcity;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;

import java.util.List;

/**
 * Created by igor.petrenko on 02.03.2018.
 */
public class TestDB {
    public static void dropDatabases( ClickHouseClient client ) {
        final String time = DateTime.now().minusDays( 2 ).toString( "YYYY-MM-dd HH:mm:ss" );
        final List<String> lines = client
            .useDatabase( "system" )
            .getLines( "SELECT database FROM (SELECT database, MAX(metadata_modification_time) AS time from system.tables GROUP BY database) WHERE time > '" + time + "'" );

        System.out.println( "lines = " + lines );

        for( String database : lines ) {
            if( "system".equals( database ) ) continue;

            client.useDatabase( database ).dropDatabase();
        }
    }

    public static String testDbName( String database ) {
        return StringUtils.replaceChars( database + "_" + Teamcity.buildPrefix(), ".-", "_" );
    }
}
