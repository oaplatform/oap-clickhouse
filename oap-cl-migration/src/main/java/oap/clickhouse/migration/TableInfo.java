package oap.clickhouse.migration;

import lombok.ToString;

import java.util.List;
import java.util.Map;

/**
 * Created by igor.petrenko on 2019-10-28.
 */
@ToString
public class TableInfo {
    public final List<ConfigField> fields;
    public final List<ConfigIndex> indexes;
    public final TableEngine tableEngine;
    public final Map<String, String> params;
    public final String name;

    public TableInfo( String name,
                      List<ConfigField> fields,
                      List<ConfigIndex> indexes,
                      TableEngine tableEngine,
                      Map<String, String> params ) {
        this.name = name;
        this.fields = fields;
        this.indexes = indexes;
        this.tableEngine = tableEngine;
        this.params = params;
    }

    public TableInfo( String name, List<ConfigField> fields, List<ConfigIndex> indexes, TableEngine tableEngine ) {
        this( name, fields, indexes, tableEngine, Map.of() );
    }
}
