/*
 * Copyright (c) Xenoss
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 */

package oap.clickhouse.migration;

public enum EngineType {
    MergeTree( true, true ),
    Memory( false, false ),
    MaterializedView( false, false ),
    AggregatingMergeTree( false, false ),
    SummingMergeTree( true, false ),
    ReplacingMergeTree( true, true ),
    Distributed( false, false );

    public final boolean supportTtl;
    public final boolean supportIndexGranularity;
    public final boolean replicated;

    EngineType( boolean supportTtl, boolean supportIndexGranularity ) {
        this.supportTtl = supportTtl;
        this.supportIndexGranularity = supportIndexGranularity;
        this.replicated = name().startsWith( "Replicated" );
    }
}
