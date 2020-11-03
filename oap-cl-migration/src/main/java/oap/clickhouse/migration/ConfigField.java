package oap.clickhouse.migration;

import com.google.common.base.Preconditions;
import oap.clickhouse.migration.FieldType.LowCardinality;
import lombok.ToString;

import java.util.Optional;

import static oap.clickhouse.migration.FieldType.ENUM;
import static oap.clickhouse.migration.FieldType.STRING;
import static oap.clickhouse.migration.FieldType.STRING_ARRAY;

/**
 * Created by igor.petrenko on 24.10.2016.
 */
@ToString
public class ConfigField {
    public final String name;
    public final FieldType type;
    public final Optional<Integer> length;
    public final Optional<String> enumName;
    public final Optional<String> materialized;
    public final Optional<Object> defaultValue;
    public final Optional<Boolean> lowCardinality;
    public final int ttl;

    public ConfigField( String name, FieldType type, Optional<? extends Number> length, Optional<Boolean> lowCardinality, Optional<String> enumName,
                        Optional<String> materialized, Optional<Object> defaultValue, int ttl ) {
        this.name = name;
        this.type = type;
        this.length = length.map( Number::intValue );
        this.lowCardinality = lowCardinality;
        this.enumName = enumName;
        this.materialized = materialized;
        this.defaultValue = defaultValue;
        this.ttl = ttl;
    }

    public static ConfigField build( String name, FieldType type ) {
        return build( name, type, false );
    }

    public static ConfigField build( String name, FieldType type, boolean lowCardinality ) {
        return new ConfigField( name, type, Optional.empty(), lowCardinality ? Optional.of( true ) : Optional.empty(),
            Optional.empty(), Optional.empty(), Optional.empty(), 0 );
    }

    public static ConfigField buildEnum( String name, String enumType ) {
        return new ConfigField( name, ENUM, Optional.empty(), Optional.empty(), Optional.of( enumType ), Optional.empty(), Optional.empty(), 0 );
    }

    public static ConfigField buildFixedString( String name, int length ) {
        return buildFixedString( name, length, false );
    }

    public static ConfigField buildFixedString( String name, int length, boolean lowCardinality ) {
        return new ConfigField( name, STRING, Optional.of( length ),
            lowCardinality ? Optional.of( true ) : Optional.empty(),
            Optional.empty(), Optional.empty(), Optional.empty(), 0 );
    }

    public static ConfigField buildFixedArrayString( String name, int length ) {
        return new ConfigField( name, STRING_ARRAY, Optional.of( length ), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), 0 );
    }

    public ConfigField withTtl( int ttl ) {
        Preconditions.checkArgument( ttl > 0 );

        return new ConfigField( name, type, length, lowCardinality, enumName, materialized, defaultValue, ttl );
    }

    public ConfigField withDefaultValue( Object defaultValue ) {
        return new ConfigField( name, type, length, lowCardinality, enumName, materialized, Optional.of( defaultValue ), ttl );
    }

    String getAddSql() {
        return "ALTER TABLE ${DATABASE}.${TABLE} ADD COLUMN " + getColumnSql() + " AFTER ${AFTER}";
    }

    String getColumnSql() {
        return name + ' ' + type.toClickHouseType( length, enumName, lowCardinality.filter( lc -> lc ).map( lc -> LowCardinality.ON ).orElse( LowCardinality.OFF ) ) + materialized
            .map( m -> " MATERIALIZED " + m )
            .orElse( defaultValue.map( dv -> " DEFAULT " + valueToSql( dv ) ).orElse( "" ) );
    }

    private String valueToSql( Object defaultValue ) {
        if( defaultValue instanceof String ) {
            return "'" + defaultValue + "'";
        } else if( defaultValue == null ) {
            return "NULL";
        }

        return defaultValue.toString();
    }

    public String getModifySql() {
        return "ALTER TABLE ${DATABASE}.${TABLE} MODIFY COLUMN " + getColumnSql();
    }

    public boolean typeEquals( String type ) {
        return this.type.toClickHouseType( length, enumName, lowCardinality.filter( lc -> lc ).map( lc -> LowCardinality.ON ).orElse( LowCardinality.OFF ) ).equals( type );
    }
}
