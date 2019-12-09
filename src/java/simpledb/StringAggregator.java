package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Map.Entry;
import java.util.PrimitiveIterator.OfDouble;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {
	private int gbFieldNo;
	private int aFieldNo;
	private HashMap<Field, Integer> resultOfGroups;
	private TupleDesc tDesc;

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    	// StringAggregator只支持COUNT操作，因为其它聚合操作对StringField没有意义。
    	if (what != Op.COUNT) {
    		throw new IllegalArgumentException("Only COUNT");
    	}
    	this.gbFieldNo = gbfield;
    	this.aFieldNo = afield;
    	this.resultOfGroups = new HashMap<Field, Integer>();
    	if (gbfield == NO_GROUPING) {
    		tDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateValue"});
    	} else {
    		tDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"groupValue", "aggregateValue"});
    	}
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	Field gbFieldVal = gbFieldNo==NO_GROUPING? _DUMMY_FIELD: tup.getField(gbFieldNo);
    	if (!resultOfGroups.containsKey(gbFieldVal)) {
    		resultOfGroups.put(gbFieldVal, 1);
    	} else {
    		resultOfGroups.replace(gbFieldVal, resultOfGroups.get(gbFieldVal)+1);	
    	}
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
    	return new OpIterator() {
    		private boolean opened = false;
    		private Iterator<Entry<Field, Integer>> it;
			
			@Override
			public void rewind() throws DbException, TransactionAbortedException {
				it = resultOfGroups.entrySet().iterator();
			}
			
			@Override
			public void open() throws DbException, TransactionAbortedException {
				opened = true;
				it = resultOfGroups.entrySet().iterator();
			}
			
			@Override
			public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
				if (!opened) {
					throw new DbException("Must open() first OR have close().");
				}
				Tuple tuple = new Tuple(tDesc);
				Entry<Field, Integer> nextEntry = it.next();
				if (gbFieldNo == NO_GROUPING) {
					tuple.setField(0, new IntField(nextEntry.getValue()));
				} else {
					tuple.setField(0, nextEntry.getKey());
					tuple.setField(1, new IntField(nextEntry.getValue()));
				}
				return tuple;
			}
			
			@Override
			public boolean hasNext() throws DbException, TransactionAbortedException {
				if (!opened) {
					throw new DbException("Must open() first OR have close().");
				}
				return it.hasNext();
			}
			
			@Override
			public TupleDesc getTupleDesc() {
				return tDesc;
			}
			
			@Override
			public void close() {
				opened = false;
			}
		};
    }


}
