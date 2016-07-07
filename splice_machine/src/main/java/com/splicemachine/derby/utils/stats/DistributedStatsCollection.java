package com.splicemachine.derby.utils.stats;

import com.splicemachine.concurrent.Clock;
import com.splicemachine.derby.iapi.sql.olap.DistributedJob;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.derby.stream.iapi.ScanSetBuilder;

import java.util.concurrent.Callable;

/**
 * Created by dgomezferro on 6/15/16.
 */
public class DistributedStatsCollection extends DistributedJob {
    ScanSetBuilder scanSetBuilder;
    String scope;

    public DistributedStatsCollection() {}

    public DistributedStatsCollection(ScanSetBuilder scanSetBuilder, String scope) {
        this.scanSetBuilder = scanSetBuilder;
        this.scope = scope;
    }

    @Override
    public Callable<Void> toCallable(OlapStatus jobStatus, Clock clock, long clientTimeoutCheckIntervalMs) {
        return new StatsCollectionJob(this, jobStatus, clock, clientTimeoutCheckIntervalMs);
    }

    @Override
    public String getName() {
        return null;
    }
}
