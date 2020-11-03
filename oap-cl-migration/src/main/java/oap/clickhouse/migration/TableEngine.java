package oap.clickhouse.migration;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Created by igor.petrenko on 2019-10-29.
 */
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
