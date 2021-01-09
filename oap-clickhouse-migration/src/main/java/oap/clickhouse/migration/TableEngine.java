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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class TableEngine {
    public final Engine engine;
    public final ArrayList<String> orderBy = new ArrayList<>();
    public String partitionBy;
    public Optional<Integer> index_granularity;

    public TableEngine( Engine engine ) {
        this( engine, null, List.of(), Optional.empty() );
    }

    public TableEngine( Engine engine, String partitionBy, List<String> orderBy, Optional<Integer> index_granularity ) {
        this.engine = engine;
        this.partitionBy = partitionBy;
        this.index_granularity = index_granularity;
        this.orderBy.addAll( orderBy );
    }

    @Override
    public String toString() {
        var ret = "Engine = " + engine;
        if( partitionBy != null ) ret += " PARTITION BY (" + partitionBy + ")";
        if( !orderBy.isEmpty() ) ret += " ORDER BY (" + String.join( ", ", orderBy ) + ")";
        if( engine.supportIndexGranularity && index_granularity != null && index_granularity.isPresent() )
            ret += " SETTINGS index_granularity = " + index_granularity.get();
        return ret;
    }

}
