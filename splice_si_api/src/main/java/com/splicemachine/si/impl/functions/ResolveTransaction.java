package com.splicemachine.si.impl.functions;

import com.splicemachine.si.api.data.ActiveConglomerate;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.storage.DataCell;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.spark_project.guava.base.Function;

import javax.annotation.Nullable;
import java.util.Iterator;

/**
 *
 * Utilize Global Cache to resolve transaction and place transactions seen into global cache
 * Modify record inline
 */
public class ResolveTransaction implements Function<ActiveConglomerate[],ActiveConglomerate[]> {
    private TxnSupplier globableCache;
    private TxnSupplier scanCache;



    public ResolveTransaction(TxnSupplier globalCache, TxnSupplier scanCache) {
        this.globableCache = globalCache;
        this.scanCache = scanCache;
    }


    @Nullable
    @Override
    public ActiveConglomerate[] apply(ActiveConglomerate[] array) {
        for (ActiveConglomerate activeConglomerate: array) {
            if (activeConglomerate == null || activeConglomerate.getEffectiveTimestamp()!=0 ) // Empty Array Element or Transaction Resolved
                break;
            if (activeConglomerate.getTransactionID2()<0) { // Collapsable Transaction
                globableCache.getTransaction()
            }
            if (activeConglomerate.getTransactionID1()>activeConglomerate.getTransactionID2()) { // Hierarchical Transaction

            }
        }
        return null;
    }
}