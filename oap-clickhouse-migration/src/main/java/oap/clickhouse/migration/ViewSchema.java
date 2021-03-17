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

import lombok.ToString;
import oap.clickhouse.ViewInfo;

import java.util.List;
import java.util.Optional;


@ToString
public class ViewSchema {
    public final String fromTable;
    public final Optional<String> toTable;
    public final boolean materialized;
    public final boolean populate;
    public final List<ViewInfo.AggregatorFunction> aggregates;
    public final Optional<String> where;
    public final String tag;

    public ViewSchema( String fromTable, boolean materialized, boolean populate, List<ViewInfo.AggregatorFunction> aggregates, String tag ) {
        this( fromTable, Optional.empty(), materialized, populate, aggregates, Optional.empty(), tag );
    }

    public ViewSchema( String fromTable, boolean materialized, boolean populate, List<ViewInfo.AggregatorFunction> aggregates, Optional<String> where, String tag ) {
        this( fromTable, Optional.empty(), materialized, populate, aggregates, where, tag );
    }

    public ViewSchema( String fromTable, Optional<String> toTable, boolean materialized, boolean populate, List<ViewInfo.AggregatorFunction> aggregates, String tag ) {
        this( fromTable, toTable, materialized, populate, aggregates, Optional.empty(), tag );
    }

    public ViewSchema( String fromTable, Optional<String> toTable, boolean materialized, boolean populate, List<ViewInfo.AggregatorFunction> aggregates, Optional<String> where, String tag ) {
        this.fromTable = fromTable;
        this.toTable = toTable;
        this.materialized = materialized;
        this.populate = populate;
        this.aggregates = aggregates;
        this.where = where;
        this.tag = tag;
    }
}
