package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {
	private DbFile table;
	private OpIterator child;
	private TransactionId tID;
	private TupleDesc tDesc;
	private boolean fetchNextCalled;

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
    	this.table = Database.getCatalog().getDatabaseFile(tableId);
    	if (!table.getTupleDesc().equals(child.getTupleDesc()))
    		throw new DbException("TupleDesc of child differs from table into which we are to insert.");
    	this.child = child;
    	this.tID = t;
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
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
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
				bufferPool.insertTuple(tID, table.getId(), child.next());
			} catch (NoSuchElementException | IOException e) {
				e.printStackTrace();
				throw new DbException("BufferPool.insertTuple() fails.");
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
    	this.child = children[0];
    }
}
