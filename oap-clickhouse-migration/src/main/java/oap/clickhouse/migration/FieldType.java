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

import oap.dictionary.Dictionaries;
import oap.dictionary.Dictionary;
import oap.util.Stream;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by igor.petrenko on 24.10.2016.
 */
public enum FieldType {
    STRING() {
        @Override
        public String toClickhouseType( Optional<Integer> length, Optional<String> enumName, LowCardinality lowCardinality ) {
            return lowCardinality.apply( length.map( l -> "FixedString(" + l + ')' ).orElse( "String" ) );
        }
    },
    DATETIME_ARRAY() {
        @Override
        public String toClickhouseType( Optional<Integer> length, Optional<String> enumName, LowCardinality lowCardinality ) {
            return array( length, enumName, DATETIME, lowCardinality );
        }
    },
    DATETIME() {
        @Override
        public String toClickhouseType( Optional<Integer> length, Optional<String> enumName, LowCardinality lowCardinality ) {
            return lowCardinality.apply( "DateTime" );
        }
    },
    DATETIME64() {
        @Override
        public String toClickhouseType( Optional<Integer> length, Optional<String> enumName, LowCardinality lowCardinality ) {
            return lowCardinality.apply( "DateTime64" );
        }
    },
    DATE() {
        @Override
        public String toClickhouseType( Optional<Integer> length, Optional<String> enumName, LowCardinality lowCardinality ) {
            return lowCardinality.apply( "Date" );
        }
    },
    BOOLEAN() {
        @Override
        public String toClickhouseType( Optional<Integer> length, Optional<String> enumName, LowCardinality lowCardinality ) {
            return lowCardinality.apply( "UInt8" );
        }
    },
    STRING_ARRAY() {
        @Override
        public String toClickhouseType( Optional<Integer> length, Optional<String> enumName, LowCardinality lowCardinality ) {
            return array( length, enumName, STRING, lowCardinality );
        }
    },
    BYTE() {
        @Override
        public String toClickhouseType( Optional<Integer> length, Optional<String> enumName, LowCardinality lowCardinality ) {
            return lowCardinality.apply( "UInt8" );
        }
    },
    INTEGER() {
        @Override
        public String toClickhouseType( Optional<Integer> length, Optional<String> enumName, LowCardinality lowCardinality ) {
            return lowCardinality.apply( "Int32" );
        }
    },
    UNSIGNED_INTEGER() {
        @Override
        public String toClickhouseType( Optional<Integer> length, Optional<String> enumName, LowCardinality lowCardinality ) {
            return lowCardinality.apply( "UInt32" );
        }
    },
    LONG() {
        @Override
        public String toClickhouseType( Optional<Integer> length, Optional<String> enumName, LowCardinality lowCardinality ) {
            return lowCardinality.apply( "Int64" );
        }
    },
    LONG_ARRAY() {
        @Override
        public String toClickhouseType( Optional<Integer> length, Optional<String> enumName, LowCardinality lowCardinality ) {
            return array( length, enumName, LONG, lowCardinality );
        }
    },
    UNSIGNED_LONG() {
        @Override
        public String toClickhouseType( Optional<Integer> length, Optional<String> enumName, LowCardinality lowCardinality ) {
            return lowCardinality.apply( "UInt64" );
        }
    },
    DOUBLE() {
        @Override
        public String toClickhouseType( Optional<Integer> length, Optional<String> enumName, LowCardinality lowCardinality ) {
            return lowCardinality.apply( "Float64" );
        }
    },
    ENUM() {
        @Override
        public String toClickhouseType( Optional<Integer> length, Optional<String> enumName, LowCardinality lowCardinality ) {
            var dictionaryName = enumName.orElse( "" );

            var level = dictionaryName.indexOf( '/' );

            List<? extends Dictionary> values;
            if( level > 0 ) {
                values = Dictionaries.getDictionary( dictionaryName.substring( 0, level ) ).getValues();

                var dLevel = dictionaryName.length() - level - 2;
                values = get( values, dLevel );
            } else {
                values = Dictionaries.getDictionary( dictionaryName ).getValues();
            }

            var sValues = new ArrayList<>( values );
            sValues.sort( Comparator.comparingInt( Dictionary::getExternalId ) );

            var statistics = sValues
                .stream()
                .mapToInt( Dictionary::getExternalId )
                .summaryStatistics();
            var enumType = statistics.getMax() >= 128 || statistics.getMin() <= -128 ? "Enum16" : "Enum8";

            return lowCardinality.apply( enumType + '(' + sValues
                .stream()
                .map( v -> '\'' + ( v.getId().equals( "UNKNOWN" ) ? "" : v.getId() ) + "' = " + v.getExternalId() )
                .collect( Collectors.joining( ", " ) ) + ")" );

        }

        private List<? extends Dictionary> get( List<? extends Dictionary> values, int dLevel ) {
            return dLevel > 0
                ? get( Stream.of( values.stream().flatMap( v -> v.getValues().stream() ) ).distinctByProperty( Dictionary::getId )
                .collect( Collectors.toList() ), dLevel - 1 )
                : values;
        }
    },
    ENUM_ARRAY() {
        @Override
        public String toClickhouseType( Optional<Integer> length, Optional<String> enumName, LowCardinality lowCardinality ) {
            return array( length, enumName, ENUM, lowCardinality );
        }
    };

    private static String array( Optional<Integer> length, Optional<String> enumName, FieldType type, LowCardinality lowCardinality ) {
        return "Array(" + type.toClickhouseType( length, enumName, lowCardinality ) + ")";
    }

    public abstract String toClickhouseType( Optional<Integer> length, Optional<String> enumName, LowCardinality lowCardinality );

    public enum LowCardinality implements Function<String, String> {
        ON() {
            @Override
            public String apply( String type ) {
                return "LowCardinality(" + type + ")";
            }
        }, OFF() {
            @Override
            public String apply( String type ) {
                return type;
            }
        };
    }
}
