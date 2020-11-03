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
        public String toClickHouseType( Optional<Integer> length, Optional<String> enumName, LowCardinality lowCardinality ) {
            return lowCardinality.apply( length.map( l -> "FixedString(" + l + ')' ).orElse( "String" ) );
        }
    },
    DATETIME_ARRAY() {
        @Override
        public String toClickHouseType( Optional<Integer> length, Optional<String> enumName, LowCardinality lowCardinality ) {
            return array( length, enumName, DATETIME, lowCardinality );
        }
    },
    DATETIME() {
        @Override
        public String toClickHouseType( Optional<Integer> length, Optional<String> enumName, LowCardinality lowCardinality ) {
            return lowCardinality.apply( "DateTime" );
        }
    },
    DATETIME64() {
        @Override
        public String toClickHouseType( Optional<Integer> length, Optional<String> enumName, LowCardinality lowCardinality ) {
            return lowCardinality.apply( "DateTime64" );
        }
    },
    DATE() {
        @Override
        public String toClickHouseType( Optional<Integer> length, Optional<String> enumName, LowCardinality lowCardinality ) {
            return lowCardinality.apply( "Date" );
        }
    },
    BOOLEAN() {
        @Override
        public String toClickHouseType( Optional<Integer> length, Optional<String> enumName, LowCardinality lowCardinality ) {
            return lowCardinality.apply( "UInt8" );
        }
    },
    STRING_ARRAY() {
        @Override
        public String toClickHouseType( Optional<Integer> length, Optional<String> enumName, LowCardinality lowCardinality ) {
            return array( length, enumName, STRING, lowCardinality );
        }
    },
    BYTE() {
        @Override
        public String toClickHouseType( Optional<Integer> length, Optional<String> enumName, LowCardinality lowCardinality ) {
            return lowCardinality.apply( "UInt8" );
        }
    },
    INTEGER() {
        @Override
        public String toClickHouseType( Optional<Integer> length, Optional<String> enumName, LowCardinality lowCardinality ) {
            return lowCardinality.apply( "Int32" );
        }
    },
    UNSIGNED_INTEGER() {
        @Override
        public String toClickHouseType( Optional<Integer> length, Optional<String> enumName, LowCardinality lowCardinality ) {
            return lowCardinality.apply( "UInt32" );
        }
    },
    LONG() {
        @Override
        public String toClickHouseType( Optional<Integer> length, Optional<String> enumName, LowCardinality lowCardinality ) {
            return lowCardinality.apply( "Int64" );
        }
    },
    LONG_ARRAY() {
        @Override
        public String toClickHouseType( Optional<Integer> length, Optional<String> enumName, LowCardinality lowCardinality ) {
            return array( length, enumName, LONG, lowCardinality );
        }
    },
    UNSIGNED_LONG() {
        @Override
        public String toClickHouseType( Optional<Integer> length, Optional<String> enumName, LowCardinality lowCardinality ) {
            return lowCardinality.apply( "UInt64" );
        }
    },
    DOUBLE() {
        @Override
        public String toClickHouseType( Optional<Integer> length, Optional<String> enumName, LowCardinality lowCardinality ) {
            return lowCardinality.apply( "Float64" );
        }
    },
    ENUM() {
        @Override
        public String toClickHouseType( Optional<Integer> length, Optional<String> enumName, LowCardinality lowCardinality ) {
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
        public String toClickHouseType( Optional<Integer> length, Optional<String> enumName, LowCardinality lowCardinality ) {
            return array( length, enumName, ENUM, lowCardinality );
        }
    };

    private static String array( Optional<Integer> length, Optional<String> enumName, FieldType type, LowCardinality lowCardinality ) {
        return "Array(" + type.toClickHouseType( length, enumName, lowCardinality ) + ")";
    }

    public abstract String toClickHouseType( Optional<Integer> length, Optional<String> enumName, LowCardinality lowCardinality );

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
