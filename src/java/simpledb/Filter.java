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
    	// �����ˣ�FilterҲ��һ��Operator��Ҳ��һ��OpIterator��
    	// ����Ҫ��ȷָ�����õ��Ǹ����open()�������������open()�����߻����޵ݹ顣
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
    	// ����ǽӿڵ�ǿ��֮����Լ���Ľӿڣ�Լ������Ϊ���������������Ҫ֪���������ͣ���SeqScan���Ǳ�ģ�����ν��������������ɵ���������Եõ�Tuple�͹��ˡ�
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
