package oap.clickhouse.migration;

import oap.clickhouse.migration.ViewInfo.AggregatorFunction;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

import static java.util.stream.Collectors.joining;

/**
 * Created by igor.petrenko on 2019-10-28.
 */
@Slf4j
public class View extends AbstractTable {
    private static final String CREATE_VIEW_QUERY = "CREATE ${MATERIALIZED} VIEW ${DATABASE}.${TABLE}${TO} ${POPULATE} AS SELECT ${FIELDS}${AGGREGATES} FROM ${DATABASE}.${FROM_TABLE} ${GROUP_BY}";

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
            "FROM_TABLE", view.fromTable,
            "TO", view.toTable.map( toTable -> " TO " + toTable ).orElse( "" )
        ) ), true );

        refresh();
        database.getTable( view.fromTable ).refresh();
    }
}
