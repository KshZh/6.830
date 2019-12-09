package simpledb;

import java.io.NotActiveException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Map.Entry;
import java.util.PrimitiveIterator.OfDouble;

import sun.font.GlyphLayout.GVData;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {
	private int gbFieldNo;
	private int aFieldNo;
	private Op op;
	// HashMap uses equals() to compare the key whether the are equal or not.
	private HashMap<Field, Integer> resultOfGroups;
	private HashMap<Field, Integer> numTuplePerGroup;
	private TupleDesc tDesc;

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbFieldNo = gbfield;
    	this.aFieldNo = afield;
    	this.op = what;
    	this.resultOfGroups = new HashMap<Field, Integer>();
    	this.numTuplePerGroup = new HashMap<Field, Integer>();
    	if (gbfield == NO_GROUPING) {
    		tDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateValue"});
    	} else {
    		tDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}, new String[]{"groupValue", "aggregateValue"});
    	}
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	Field gbFieldVal = gbFieldNo==NO_GROUPING? _DUMMY_FIELD: tup.getField(gbFieldNo);
    	int aFieldVal = ((IntField)tup.getField(aFieldNo)).getValue();
    	if (!numTuplePerGroup.containsKey(gbFieldVal)) {
    		numTuplePerGroup.put(gbFieldVal, 1);
    	} else {
    		numTuplePerGroup.replace(gbFieldVal, numTuplePerGroup.get(gbFieldVal)+1);
    	}
//    	if (!resultOfGroups.containsKey(gbFieldVal)) {
//    		resultOfGroups.put(gbFieldVal, 0);
//    	}
		Integer oldA = resultOfGroups.get(gbFieldVal);
    	switch (op) {
		case MIN:
			if (oldA == null) {
				resultOfGroups.put(gbFieldVal, aFieldVal);
			} else if (aFieldVal<oldA) {
				resultOfGroups.replace(gbFieldVal, aFieldVal);
			}
			// 错误！如果一个已存在的key的最小值就是0呢?
//			if (oldA==0 || aFieldVal<oldA) {
//				resultOfGroups.replace(gbFieldVal, aFieldVal);
//			}
			break;
		case MAX:
			if (oldA == null) {
				resultOfGroups.put(gbFieldVal, aFieldVal);
			} else if (aFieldVal>oldA) {
				resultOfGroups.replace(gbFieldVal, aFieldVal);
			}
			break;
		case SUM:
		case AVG:
			if (oldA == null) {
				resultOfGroups.put(gbFieldVal, aFieldVal);
			} else {
				resultOfGroups.replace(gbFieldVal, oldA+aFieldVal);
			}
			break;
		case COUNT:
			if (oldA == null) {
				resultOfGroups.put(gbFieldVal, 1);
			} else {
				resultOfGroups.replace(gbFieldVal, oldA+1);
			}
			break;
		default:
			break;
		}
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
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
			
			/*
			@Override
			public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
				if (!opened) {
					throw new DbException("Must open() first OR have close().");
				}
				Tuple tuple = new Tuple(tDesc);
				Entry<Field, Integer> nextEntry = it.next(); // it.next()可能会抛出NoSuchElementException异常。
				if (gbFieldNo == NO_GROUPING) {
					if (op == Op.AVG) {
						tuple.setField(0, new IntField(nextEntry.getValue()/numTuplePerGroup.get(nextEntry.getKey())));
					} else {
						tuple.setField(0, new IntField(nextEntry.getValue()));
					}
					return tuple;
				}
				tuple.setField(0, nextEntry.getKey()); // 只有一个StringField/IntField的实例对象，因为只读不可写，所以没问题。
				if (op == Op.AVG) {
					tuple.setField(1, new IntField(nextEntry.getValue()/numTuplePerGroup.get(nextEntry.getKey())));
				} else {
					tuple.setField(1, new IntField(nextEntry.getValue()));
				}
				return tuple;
			}
			*/
			
			// 消除重复代码后，
			@Override
			public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
				if (!opened) {
					throw new DbException("Must open() first OR have close().");
				}
				Tuple tuple = new Tuple(tDesc);
				Entry<Field, Integer> nextEntry = it.next(); // it.next()可能会抛出NoSuchElementException异常。
				int val = op==op.AVG? nextEntry.getValue()/numTuplePerGroup.get(nextEntry.getKey()): nextEntry.getValue();
				if (gbFieldNo == NO_GROUPING) {
					tuple.setField(0, new IntField(val));
				} else {
					tuple.setField(0, nextEntry.getKey());
					tuple.setField(1, new IntField(val));
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
