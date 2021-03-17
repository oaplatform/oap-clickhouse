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

import com.fasterxml.jackson.annotation.JsonCreator;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import oap.clickhouse.ConfigIndex;
import oap.clickhouse.Engine;
import oap.clickhouse.TableEngine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

public class TableConfiguration extends TableEngine {
    public final ArrayList<ConfigIndex> indexes = new ArrayList<>();
    public final HashMap<String, FieldOption> fieldOptions = new HashMap<>();
    public TtlTableField ttl;
    public Integer tablePerThread;

    public TableConfiguration( Engine engine ) {
        super( engine );
    }

    @JsonCreator
    public TableConfiguration( Engine engine, List<String> partitionBy, List<String> orderBy, Optional<Integer> indexGranularity ) {
        super( engine, partitionBy, orderBy, indexGranularity );
    }

    public FieldOption getOptions( String fieldName, Defaults.Tables defaults ) {
        var options = fieldOptions.get( fieldName );
        if( options != null ) return options;

        options = defaults.fieldOptions.get( fieldName );
        if( options != null ) return options;

        return FieldOption.DEFAULT;
    }

    @ToString
    @EqualsAndHashCode
    @AllArgsConstructor
    public static class TtlTableField {
        public final String field;
        public final long duration;

        public int getTtl() {
            return ( int ) ( duration / 1000 );
        }
    }
}
