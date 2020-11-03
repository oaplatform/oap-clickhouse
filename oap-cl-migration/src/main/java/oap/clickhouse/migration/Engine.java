package oap.clickhouse.migration;

/**
 * Created by igor.petrenko on 2019-10-29.
 */
public enum Engine {
    MergeTree( true, true ),
    Memory( false, false ),
    MaterializedView( false, false ),
    AggregatingMergeTree( false, false ),
    SummingMergeTree( false, false ),
    ReplacingMergeTree( false, false );

    public final boolean supportTtl;
    public final boolean supportIndexGranularity;

    private Engine( boolean supportTtl, boolean supportIndexGranularity ) {
        this.supportTtl = supportTtl;
        this.supportIndexGranularity = supportIndexGranularity;
    }
}
