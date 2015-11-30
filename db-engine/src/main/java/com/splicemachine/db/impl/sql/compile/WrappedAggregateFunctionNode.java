package com.splicemachine.db.impl.sql.compile;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.AggregateDefinition;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;

/**
 * @author Jeff Cunningham
 *         Date: 10/2/14
 */
public class WrappedAggregateFunctionNode extends WindowFunctionNode {
    private AggregateNode aggregateFunction;
    /**
     * Initializer. QueryTreeNode override.
     *
     * @param arg1 the window definition
     * @param arg2 the wrapped aggregate function
     * @throws com.splicemachine.db.iapi.error.StandardException
     */
    public void init(Object arg1, Object arg2) throws StandardException {
        super.init(arg1, null);
        aggregateFunction = (AggregateNode) arg2;
        this.aggregateName = aggregateFunction.aggregateName;
        this.operator = aggregateFunction.operator;
    }

    @Override
    public String getName() {
        return aggregateFunction.getAggregateName();
    }

    @Override
     public ValueNode getNewNullResultExpression() throws StandardException {
         return aggregateFunction.getNewNullResultExpression();
     }

    @Override
    public DataTypeDescriptor getTypeServices() {
        return aggregateFunction.getTypeServices();
    }

    @Override
    public List<ValueNode> getOperands() {
        ValueNode wrappedOperand = aggregateFunction.operand;
        return (wrappedOperand != null ? Lists.newArrayList(wrappedOperand) : Collections.EMPTY_LIST);
    }

    @Override
    public void replaceOperand(ValueNode oldVal, ValueNode newVal) {
        // TODO: JC - what else? bind?
        aggregateFunction.operand = newVal;
    }

    /**
     * Overridden to redirect the call to the wrapped aggregate node.
     * @param tableNumber The tableNumber for the new ColumnReference
     * @param nestingLevel this node's nesting level
     * @return the new CR
     * @throws StandardException
     */
    @Override
    public ValueNode replaceCallWithColumnReference(int tableNumber, int nestingLevel) throws StandardException {
        ColumnReference node = (ColumnReference) aggregateFunction.replaceAggregatesWithColumnReferences(
            (ResultColumnList) getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN_LIST,
                getContextManager()),
            tableNumber);

        // Mark the ColumnReference as being generated to replace a call to
        // a window function
        node.markGeneratedToReplaceWindowFunctionCall();
        return node;
    }

    /**
      * QueryTreeNode override. Prints the sub-nodes of this object.
      *
      * @param depth The depth of this node in the tree
      * @see QueryTreeNode#printSubNodes
      */
     public void printSubNodes(int depth) {
         if (SanityManager.DEBUG) {
             super.printSubNodes(depth);

             printLabel(depth, "aggregate: ");
             aggregateFunction.treePrint(depth + 1);
         }
     }

    @Override
    public ValueNode bindExpression(FromList fromList,
                                    SubqueryList subqueryList,
                                    List<AggregateNode> aggregateVector) throws StandardException {
        // DB-2086 - Vector.remove() calls node1.isEquivalent(node2), not node1.equals(node2), which
        // returns true for identical aggregate nodes removing the first aggregate, not necessarily
        // this one. We need to create a tmp Vector and add all Agg nodes found but this delegate by
        // checking for object identity (==)
        List<AggregateNode> tmp = new ArrayList<>();
        aggregateFunction.bindExpression(fromList,subqueryList,tmp);

        // We don't want to be in this aggregateVector - we add all aggs found during bind except
        // this delegate.
        // We want to bind the wrapped aggregate (and operand, etc) but we don't want to show up
        // in this list as an aggregate. The list will be handed to GroupByNode, which we don't
        // want doing the work.  Window function code will handle the window function aggregates
        for (AggregateNode aggFn : tmp) {
            if (aggregateFunction != aggFn) {
                aggregateVector.add(aggFn);
            }
        }

        // Now that delegate is bound, set some required fields on this
        // TODO: JC What all is required?
        this.operator = aggregateFunction.operator;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        WrappedAggregateFunctionNode that = (WrappedAggregateFunctionNode) o;

        return aggregateFunction.equals(that.aggregateFunction);

    }

    @Override
    public int hashCode() {
        return aggregateFunction.hashCode();
    }

    /**
     * Get the generated ResultColumn where this
     * aggregate now resides after a call to
     * replaceAggregatesWithColumnReference().
     *
     * @return the result column
     */
     @Override
     AggregateDefinition getAggregateDefinition() {
         return aggregateFunction.getAggregateDefinition();
     }
     @Override
     public boolean isDistinct() {
         return aggregateFunction.isDistinct();
     }

     @Override
     public String getAggregatorClassName() {
         return aggregateFunction.getAggregatorClassName();
     }

     @Override
     public String getAggregateName() {
         return aggregateFunction.getAggregateName();
     }

     @Override
     public ResultColumn getNewAggregatorResultColumn(DataDictionary dd) throws StandardException {
         return aggregateFunction.getNewAggregatorResultColumn(dd);
     }

     @Override
     public ResultColumn getNewExpressionResultColumn(DataDictionary dd) throws StandardException {
         return aggregateFunction.getNewExpressionResultColumn(dd);
     }

     @Override
     public void generateExpression(ExpressionClassBuilder acb, MethodBuilder mb) throws StandardException {
         aggregateFunction.generateExpression(acb, mb);
     }

     @Override
     public String getSQLName() {
         return aggregateFunction.getSQLName();
     }

     @Override
     public ColumnReference getGeneratedRef() {
         return aggregateFunction.getGeneratedRef();
     }

     @Override
     public ResultColumn getGeneratedRC() {
         return aggregateFunction.getGeneratedRC();
     }

}
