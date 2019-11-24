package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	private File file;
	private TupleDesc tDesc;
	private int numPages;
	private int tableId;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	this.file = f;
    	this.tDesc = td;
    	this.numPages = (int) Math.floor(file.length()/BufferPool.getPageSize());
    	this.tableId = file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return tableId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
    	byte[] buf = new byte[BufferPool.getPageSize()];
    	try {
	    	RandomAccessFile iFile = new RandomAccessFile(file, "r");
	    	iFile.seek(pid.getPageNumber()*BufferPool.getPageSize());
	    	iFile.read(buf);
	    	if (tableId == 0)
	    		tableId = pid.getTableId();
	    	return new HeapPage((HeapPageId) pid, buf);
		} catch (IOException e) {
			e.printStackTrace();
			return null; // TODO
		}
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
    	return numPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
        	boolean opened = false;
        	boolean closed = false;
        	int pgNo = 0;
        	Iterator<Tuple> it = null;
			
			@Override
			public void rewind() throws DbException, TransactionAbortedException {
				// TODO Auto-generated method stub
				pgNo = 0;
				it = null;
			}
			
			@Override
			public void open() throws DbException, TransactionAbortedException {
				// TODO Auto-generated method stub
				opened = true;
			}
			
			@Override
			public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
				// TODO Auto-generated method stub
				if (!opened || closed)
					throw new NoSuchElementException();
				// 这里假定caller每次都会先检查hasNext()再调用next()，所以没有做更多的检查。
				return it.next();
			}
			
			@Override
			public boolean hasNext() throws DbException, TransactionAbortedException {
				// TODO Auto-generated method stub
				if (!opened || closed)
					return false;
				if (pgNo > numPages)
					return false;
				if (it == null)
					it = ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(tableId, pgNo), null)).iterator();
				if (!it.hasNext() && pgNo+1<numPages) { // XXX 注意pgNo是从0开始的，而numPages是Page数组大小。
					pgNo++;
					it = ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(tableId, pgNo), null)).iterator();
				}
				return it.hasNext();
			}
			
			@Override
			public void close() {
				// TODO Auto-generated method stub
				closed = true;
			}
		};
    }

}

