package simpledb;

import java.util.*;

import simpledb.Aggregator.Op;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {
	private OpIterator childIterator;
	private Aggregator aggregator;
	private int gbFieldNo;
	private int aFieldNo;
	private TupleDesc tDesc;
	private Op op;

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
    	// some code goes here
    	this.gbFieldNo = gfield;
    	this.aFieldNo = afield;
    	this.op = aop;
    	Type gType = null;
    	TupleDesc tDesc = child.getTupleDesc();
    	Type aType = tDesc.getFieldType(afield);
    	if (gfield != Aggregator.NO_GROUPING) {
        	gType = tDesc.getFieldType(gfield);
        	this.tDesc = new TupleDesc(new Type[]{gType, aType}, new String[]{tDesc.getFieldName(gfield), tDesc.getFieldName(afield)});
    	} else {
    		this.tDesc = new TupleDesc(new Type[]{aType}, new String[]{tDesc.getFieldName(afield)});
    	}
    	if (aType == Type.STRING_TYPE) {
    		aggregator = new StringAggregator(gfield, gType, afield, aop);
    	} else {
    		aggregator = new IntegerAggregator(gfield, gType, afield, aop);
    	}
    	try {
    		child.open();
			while (child.hasNext()) {
				this.aggregator.mergeTupleIntoGroup(child.next());
			}
			child.close();
		} catch (DbException e) {
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			e.printStackTrace();
		}
    	this.childIterator = this.aggregator.iterator();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
    	// some code goes here
    	return gbFieldNo;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
    	// some code goes here
    	if (gbFieldNo != aggregator.NO_GROUPING) {
    		return tDesc.getFieldName(0);
    	}
    	return null;
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
    	// some code goes here
    	return aFieldNo;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
		// some code goes here
		return tDesc.getFieldName(1);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
		// some code goes here
		return op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
		return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
    	// some code goes here
    	super.open();
    	childIterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		// some code goes here
    	if (childIterator.hasNext()) {
    		return childIterator.next();
    	}
		return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	// some code goes here
    	childIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
		// some code goes here
		return tDesc;
    }

    public void close() {
    	// some code goes here
    	super.close();
    	childIterator.close();
    }

    @Override
    public OpIterator[] getChildren() {
		// some code goes here
		return new OpIterator[]{childIterator};
    }

    @Override
    public void setChildren(OpIterator[] children) {
    	// some code goes here
    	// TODO 这里有bug，必须先把this.aggregator清空，否则就叠加上去了。
    	OpIterator child = children[0];
    	try {
    		child.open();
			while (child.hasNext()) {
				this.aggregator.mergeTupleIntoGroup(child.next());
			}
			child.close();
		} catch (DbException e) {
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			e.printStackTrace();
		}
    	this.childIterator = this.aggregator.iterator();
    }
    
}
