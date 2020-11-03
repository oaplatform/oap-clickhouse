package oap.clickhouse.migration;

import oap.clickhouse.DefaultClickHouseClient;
import oap.testng.Env;
import oap.testng.Teamcity;
import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

/**
 * Created by igor.petrenko on 2019-10-28.
 */
public class BaseDatabaseTest {
    public static final String DB = "table_db_" + StringUtils.replaceChars( Teamcity.buildPrefix(), ".-", "_" );
    private static final String HOST = Env.getEnvOrDefault( "CLICKHOUSE_HOST", "localhost" );
    protected Database database;

    protected static ViewInfo.Field _f( String field ) {
        return new ViewInfo.Field( field, null );
    }

    protected static ViewInfo.Field _f( String function, String alias ) {
        return new ViewInfo.Field( function, alias );
    }

    @BeforeMethod
    public void beforeMethod() {
        database = new DefaultClickHouseClient( HOST, 8123, DB ).getDatabase();
        database.createIfNotExists();
        if( database.getTable( "TEST" ).exists() )
            database.getTable( "TEST" ).drop();
    }

    @AfterMethod
    public void afterMethod() {

        try {
            database.drop();
        } catch( Exception e ) {
            e.printStackTrace();
        }
    }
}
