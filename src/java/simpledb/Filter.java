package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {
	private Predicate predicate;
	private OpIterator childIterator;

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // some code goes here
    	this.predicate = p;
    	this.childIterator = child;
    }

    public Predicate getPredicate() {
        // some code goes here
        return predicate;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return childIterator.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
    	// 别忘了，Filter也是一个Operator，也是一个OpIterator。
    	// 这里要明确指出调用的是父类的open()，而不是子类的open()，后者会无限递归。
    	super.open();
    	childIterator.open();
    }

    public void close() {
        // some code goes here
    	super.close();
    	childIterator.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	childIterator.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
    	Tuple tuple;
    	// 这就是接口的强大之处，约定的接口，约定的行为、输入输出，不需要知道具体类型，是SeqScan还是别的，无所谓，反正迭代这个可迭代对象可以得到Tuple就够了。
    	while (childIterator.hasNext()) {
    		tuple = childIterator.next();
    		if (predicate.filter(tuple)) {
    			return tuple;
    		}
    	}
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] {childIterator};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    	this.childIterator = children[0];
    }

}
