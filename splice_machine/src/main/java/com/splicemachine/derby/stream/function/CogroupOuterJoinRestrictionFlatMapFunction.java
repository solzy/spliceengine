package com.splicemachine.derby.stream.function;

import org.sparkproject.guava.collect.Iterables;
import org.sparkproject.guava.collect.Sets;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by jleach on 4/30/15.
 */
public class CogroupOuterJoinRestrictionFlatMapFunction<Op extends SpliceOperation> extends SpliceJoinFlatMapFunction<Op, Tuple2<ExecRow,Tuple2<Iterable<LocatedRow>,Iterable<LocatedRow>>>,LocatedRow> {
    protected OuterJoinRestrictionFlatMapFunction outerJoinRestrictionFlatMapFunction;
    protected LocatedRow leftRow;
    public CogroupOuterJoinRestrictionFlatMapFunction() {
        super();
    }

    public CogroupOuterJoinRestrictionFlatMapFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public Iterable<LocatedRow> call(Tuple2<ExecRow,Tuple2<Iterable<LocatedRow>, Iterable<LocatedRow>>> tuple) throws Exception {
        checkInit();
//        Set<LocatedRow> rightSide = Sets.newHashSet(tuple._2._2); // Memory Issue, HashSet ?
        Iterable<LocatedRow> returnRows = new ArrayList<>();
        for(Object a_1 : tuple._2._1){
            returnRows=Iterables.concat(returnRows,outerJoinRestrictionFlatMapFunction.call(new Tuple2<>(a_1,tuple._2._2)));
        }
        return returnRows;
    }
    @Override
    protected void checkInit() {
        if (!initialized)
            outerJoinRestrictionFlatMapFunction = new OuterJoinRestrictionFlatMapFunction<>(operationContext);
        super.checkInit();
    }

}