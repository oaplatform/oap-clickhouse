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
import lombok.ToString;
import oap.clickhouse.migration.FieldType.LowCardinality;

import java.util.Optional;

import static oap.clickhouse.migration.FieldType.ENUM;
import static oap.clickhouse.migration.FieldType.STRING;
import static oap.clickhouse.migration.FieldType.STRING_ARRAY;

@ToString
public class ConfigField {
    public final String name;
    public final FieldType type;
    public final Optional<Integer> length;
    public final Optional<String> enumName;
    public final Optional<String> materialized;
    public final Optional<Object> defaultValue;
    public final Optional<Boolean> lowCardinality;
    public final String codec;
    public final int ttl;

    public ConfigField( String name, FieldType type, Optional<? extends Number> length,
                        Optional<Boolean> lowCardinality,
                        String codec,
                        Optional<String> enumName,
                        Optional<String> materialized, Optional<Object> defaultValue, int ttl ) {
        Preconditions.checkNotNull( codec );

        this.name = name;
        this.type = type;
        this.length = length.map( Number::intValue );
        this.lowCardinality = lowCardinality;
        this.codec = codec.trim();
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
            "",
            Optional.empty(), Optional.empty(), Optional.empty(), 0 );
    }

    public static ConfigField buildEnum( String name, String enumType ) {
        return new ConfigField( name, ENUM, Optional.empty(), Optional.empty(), "", Optional.of( enumType ), Optional.empty(), Optional.empty(), 0 );
    }

    public static ConfigField buildFixedString( String name, int length ) {
        return buildFixedString( name, length, false );
    }

    public static ConfigField buildFixedString( String name, int length, boolean lowCardinality ) {
        return new ConfigField( name, STRING, Optional.of( length ),
            lowCardinality ? Optional.of( true ) : Optional.empty(), "",
            Optional.empty(), Optional.empty(), Optional.empty(), 0 );
    }

    public static ConfigField buildFixedArrayString( String name, int length ) {
        return new ConfigField( name, STRING_ARRAY, Optional.of( length ), Optional.empty(), "", Optional.empty(), Optional.empty(), Optional.empty(), 0 );
    }

    public ConfigField withTtl( int ttl ) {
        Preconditions.checkArgument( ttl > 0 );

        return new ConfigField( name, type, length, lowCardinality, codec, enumName, materialized, defaultValue, ttl );
    }

    public ConfigField withDefaultValue( Object defaultValue ) {
        return new ConfigField( name, type, length, lowCardinality, codec, enumName, materialized, Optional.of( defaultValue ), ttl );
    }

    public ConfigField withCodec( String codec ) {
        return new ConfigField( name, type, length, lowCardinality, codec,
            enumName, materialized, defaultValue, ttl );
    }

    String getAddSql() {
        return "ALTER TABLE ${DATABASE}.${TABLE} ADD COLUMN " + getColumnSql() + "${AFTER_OR_FIRST}";
    }

    String getColumnSql() {
        return name + ' ' + type.toClickhouseType( length, enumName, lowCardinality.filter( lc -> lc ).map( lc -> LowCardinality.ON ).orElse( LowCardinality.OFF ) ) + materialized
            .map( m -> " MATERIALIZED " + m )
            .orElse( defaultValue.map( dv -> " DEFAULT " + valueToSql( dv ) ).orElse( "" ) )
            + ( codec.isEmpty() ? "" : " " + codec );
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
        return "ALTER TABLE ${DATABASE}.${TABLE} MODIFY COLUMN " + getColumnSql() + "${AFTER_OR_FIRST}";
    }

    public boolean typeEquals( String type, String codec ) {
        return this.type.toClickhouseType( length, enumName, lowCardinality.filter( lc -> lc ).map( lc -> LowCardinality.ON ).orElse( LowCardinality.OFF ) ).equals( type )
            && this.codec.equals( codec );
    }
}
