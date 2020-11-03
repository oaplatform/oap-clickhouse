package oap.clickhouse;

/**
 * Created by igor.petrenko on 24.10.2016.
 */
public class ClickHouseException extends RuntimeException {
    public final int code;
    public final String body;

    public ClickHouseException( String message, int code, String body ) {
        super( message );
        this.code = code;
        this.body = body != null ? body : message;
    }

    public ClickHouseException( Throwable cause ) {
        super( cause );
        this.code = 0;
        this.body = cause.getMessage();
    }
}
