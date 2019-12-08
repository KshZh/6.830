package simpledb;

import static org.junit.Assert.assertNotNull;

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
	private File file; // 磁盘文件，真正存储数据的地方。
	private TupleDesc tDesc;
	private int numPages;
	private int tableID;

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
    	this.tableID = file.getAbsoluteFile().hashCode();
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
        return tableID;
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
	    	if (tableID == 0)
	    		tableID = pid.getTableId();
	    	return new HeapPage((HeapPageId) pid, buf);
		} catch (IOException e) {
			throw new RuntimeException(e);
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
    	BufferPool bufferPool = Database.getBufferPool();
    	ArrayList<Page> arrayList = new ArrayList<Page>();
    	for (int i = 0; i < numPages; i++) {
			HeapPage page = (HeapPage) bufferPool.getPage(tid, new HeapPageId(tableID, i), null);
			if (page.getNumEmptySlots() != 0) {
				page.insertTuple(t);
				arrayList.add(page);
				return arrayList;
			}
		}
    	numPages++;
    	HeapPage page = (HeapPage) bufferPool.getPage(tid, new HeapPageId(tableID, numPages-1), null);
    	page.insertTuple(t);
		arrayList.add(page);
		return arrayList;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
    }

    // see DbFile.java for javadocs
	// 这使得客户可以得到一个表/DbFile的迭代器，迭代表中的Tuple。
    public DbFileIterator iterator(TransactionId tid) {
        return new DbFileIterator() {
        	private final BufferPool pool = Database.getBufferPool();
        	// private boolean opened = false; // 利用`pgNo>=0`这个不变量，可以用pgNo的负值作为open()和close()的标志。
        	private int pgNo = -1;
        	private Iterator<Tuple> child = null;
			
			@Override
			public void rewind() throws DbException, TransactionAbortedException {
				pgNo = 0;
				child = null;
			}
			
			@Override
			public void open() throws DbException, TransactionAbortedException {
				pgNo = 0;
			}
			
			@Override
			public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
				if (pgNo<0 || !hasNext())
					throw new NoSuchElementException();
				return child.next();
			}
			
			@Override
			public boolean hasNext() throws DbException, TransactionAbortedException {
				if (pgNo<0 || pgNo>numPages)
					return false;
				// 这里要注意检查边界，客户会一直调用hasNext()直到返回False，甚至返回False后继续调用，我们无法保证客户的行为。
				// XXX 还要注意，`child==null`只会在迭代器最开始时成立一次（如果不rewind()的话），之后如果`!child.hasNext()`成立，
				// 也就是说这个Page的有效的（查header的bitmap）Tuple已经遍历完了，而且这时该DbFile还有Page的话，就要读入下一个Page，
				// 继续遍历。
				if ((child==null || !child.hasNext()) && pgNo<numPages) {
					child = ((HeapPage) pool.getPage(tid, new HeapPageId(tableID, pgNo++), Permissions.READ_ONLY)).iterator();
				}
				return child!=null && child.hasNext(); // 短路操作，确保不会空指针异常。
			}
			
			@Override
			public void close() {
				pgNo = -1;
			}
		};
    }

}

