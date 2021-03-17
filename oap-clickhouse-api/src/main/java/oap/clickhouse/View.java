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

package oap.clickhouse;

import lombok.extern.slf4j.Slf4j;
import oap.clickhouse.ViewInfo.AggregatorFunction;

import java.util.Map;

import static java.util.stream.Collectors.joining;

@Slf4j
public class View extends AbstractTable {
    private static final String CREATE_VIEW_QUERY = "CREATE ${MATERIALIZED} VIEW ${DATABASE}.${TABLE}${TO} ${POPULATE} AS SELECT ${FIELDS}${AGGREGATES} FROM ${DATABASE}.${FROM_TABLE}${WHERE} ${GROUP_BY}";

    public View( Database database, String name ) {
        super( database, name );
    }

    public void create( ViewInfo view, String engine ) {
        var groupByStr = view.fields.stream().map( ViewInfo.Field::getFieldWithAlias ).collect( joining( ", " ) );
        var fieldsStr = view.fields.stream().map( ViewInfo.Field::getFieldName ).collect( joining( ", " ) );
        var aggregateNameStr = view.aggregates.stream().map( AggregatorFunction::toString ).collect( joining( "," ) );

        database.client.execute( buildQuery( CREATE_VIEW_QUERY, Map.of(
            "MATERIALIZED", view.materialized ? "MATERIALIZED" : "",
            "POPULATE", view.populate ? "Engine = " + engine + " POPULATE" : "",
            "FIELDS", fieldsStr + ( view.aggregates.isEmpty() ? "" : "," + aggregateNameStr ),
            "GROUP_BY", !view.aggregates.isEmpty() ? "GROUP BY " + groupByStr : "",
            "WHERE", view.where.map( w -> " WHERE " + w ).orElse( "" ),
            "FROM_TABLE", view.fromTable,
            "TO", view.toTable.map( toTable -> " TO " + toTable ).orElse( "" )
        ) ), true );

        refresh();
        database.getTable( view.fromTable ).refresh();
    }
}
