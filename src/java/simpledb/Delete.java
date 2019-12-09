package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {
	private OpIterator child;
	private TransactionId tID;
	private TupleDesc tDesc;
	private boolean fetchNextCalled;

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
    	this.tID = t;
    	this.child = child;
    	this.tDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
    	this.fetchNextCalled = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.open();
    	super.open();
    }

    public void close() {
        // some code goes here
    	child.close();
    	super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if (fetchNextCalled)
    		return null;
    	Tuple tuple = new Tuple(tDesc);
    	BufferPool bufferPool = Database.getBufferPool();
    	int count = 0;
    	while (child.hasNext()) {
    		try {
				bufferPool.deleteTuple(tID, child.next());
			} catch (NoSuchElementException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				throw new DbException("BufferPool.deleteTuple() fails.");
			}
    		count++;
    	}
    	tuple.setField(0, new IntField(count));
    	fetchNextCalled = true;
		return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    	child = children[0];
    }

}
