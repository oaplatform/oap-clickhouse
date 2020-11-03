package oap.clickhouse.migration;

import com.fasterxml.jackson.annotation.JsonCreator;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by igor.petrenko on 2020-03-23.
 */
@ToString
@EqualsAndHashCode
public class ConfigIndex {
    public final String name;
    public final ArrayList<String> fields;
    public final String type;
    public final int granularity;

    @JsonCreator
    public ConfigIndex( String name, List<String> fields, String type, int granularity ) {
        this.name = name;
        this.fields = new ArrayList<>( fields );
        this.type = type;
        this.granularity = granularity;
    }

    public static String set() {
        return set( 0 );
    }

    public static String minmax() {
        return "minmax";
    }

    public static String set( int maxRows ) {
        return "set(" + maxRows + ")";
    }

    public static ConfigIndex index( String name, List<String> fields, String type, int granularity ) {
        return new ConfigIndex( name, fields, type, granularity );
    }

    public String getIndexSql() {
        return "INDEX " + name + " (" + String.join( ", ", fields ) + ") TYPE " + type + " GRANULARITY " + granularity;
    }
}
