# 6.830 Lab 1: SimpleDB

[course-info-2018](https://link.zhihu.com/?target=https%3A//github.com/MIT-DB-Class/course-info-2018/)

[simple-db-hw](https://link.zhihu.com/?target=https%3A//github.com/MIT-DB-Class/simple-db-hw)

SimpleDB是一个简单的DBMS。

**测试驱动，没有思路，或有什么疑惑，看测试用例！**

**面向接口编程！**

**我们应该更侧重于接口，设定接口，设置接口中的操作的输入输出，当对该接口的实现经过测试基本正确工作后，我们不应该总是纠结、回想接口的具体实现，而是应该、甚至可以把已经正确工作的实现抛在脑后，然后只关注接口来进行编程。我们的变量也应该更多地声明为接口类型，而不是实现接口的具体类型。**

**Here's a rough outline of one way you might proceed with your SimpleDB implementation:**

------

- Implement the classes to manage tuples, namely Tuple, TupleDesc. We have already implemented Field, IntField, StringField, and Type for you. Since you only need to support integer and (fixed length) string fields and fixed length tuples, these are straightforward.
- Implement the Catalog (this should be very simple).
- Implement the BufferPool constructor and the getPage() method.
- Implement the access methods, HeapPage and HeapFile and associated ID classes. A good portion of these files has already been written for you.
- Implement the operator SeqScan.
- At this point, you should be able to pass the ScanTest system test, which is the goal for this lab.

## 2. SimpleDB Architecture and Implementation Guide

SimpleDB consists of:

- Classes that represent fields, tuples, and tuple schemas;
- Classes that apply predicates and conditions to tuples;
- One or more access methods (e.g., heap files) that store relations on disk and provide a way to iterate through tuples of those relations;
- A collection of operator classes (e.g., select, join, insert, delete, etc.) that process tuples;
- A buffer pool that caches active tuples and pages in memory and handles concurrency control and transactions (neither of which you need to worry about for this lab); and,
- A catalog that stores information about available tables and their schemas.

SimpleDB does not include many things that you may think of as being a part of a "database." In particular, SimpleDB does not have:

- (In this lab), a SQL front end or parser that allows you to type queries directly into SimpleDB. Instead, queries are built up by chaining a set of operators together into a hand-built query plan (see [Section 2.7](https://github.com/MIT-DB-Class/course-info-2018/blob/master/lab1.md#query_walkthrough)). We will provide a simple parser for use in later labs.
- Views.
- Data types except integers and fixed length strings.
- (In this lab) Query optimizer.
- (In this lab) Indices.

### 2.1. The Database Class

The Database class provides access to a collection of static objects that are the global state of the database. In particular, this includes methods to access the catalog (the list of all the tables in the database), the buffer pool (the collection of database file pages that are currently resident in memory), and the log file.

### 2.2. Fields and Tuples

TupleDesc实际上也就是一个schema，schema是键值对`<attribute name: domain>`的集合，其中attribute name表达了该属性的含义，domain是属性可取值的类型和范围。表/关系是一个schema的实例，是元组(tuple)的集合，tuple则存储了具体的属性值，在SimpleDB中，这个属性值就是Field对象。

比如，存储一个人的信息的表，它的schema可能是`{<name: string>, <age: int[0, 200]>}`，它是一个描述性的东西，我并不能通过这个schema查到李四多少岁了，但我可以查询由这个schema描述的表，该表中存储了真正能被处理的数据，表中也许有这样一个tuple：{"李四", 32}，那么通过查表我就知道李四的年龄是32岁。

总的来说，schema就是具体的表/关系的抽象描述，类似于OOP中的类与类实例。

#### Interface:

##### Field

```java
/**
 * Interface for values of fields in tuples in SimpleDB.
 */
public interface Field extends Serializable{
    /**
     * Write the bytes representing this field to the specified
     * DataOutputStream.
     * @see DataOutputStream
     * @param dos The DataOutputStream to write to.
     */
    void serialize(DataOutputStream dos) throws IOException;

    /**
     * Compare the value of this field object to the passed in value.
     * @param op The operator
     * @param value The value to compare this Field to
     * @return Whether or not the comparison yields true.
     */
    public boolean compare(Predicate.Op op, Field value);

    /**
     * Returns the type of this field (see {@link Type#INT_TYPE} or {@link Type#STRING_TYPE}
     * @return type of this field
     */
    public Type getType();
    
    /**
     * Hash code.
     * Different Field objects representing the same value should probably
     * return the same hashCode.
     */
    public int hashCode();
    public boolean equals(Object field);

    public String toString();
}
```

**Exercise 1.**

- src/simpledb/TupleDesc.java

  ```java
  public class TupleDesc implements Serializable {
  	private ArrayList<TDItem> tdItems; // 使用容器的好处之一是，使用容器的类一般可以很容易借助容器的迭代器构造出该类的迭代器。
      
          public static class TDItem implements Serializable {
      	    private static final long serialVersionUID = 1L;
              
      	    public final Type fieldType;
      	    public final String fieldName;
      	}
  }
  ```
  
  通过测试TupleDescTest。
  
- src/simpledb/Tuple.java

  ```java
  public class Tuple implements Serializable {
  	private TupleDesc tDesc;
  	private ArrayList<Field> fields;
  	private RecordId rid;
      
  	// getters and setters ...
  }
  ```

  通过测试TupleTest。

### 2.3. Catalog

The catalog (class `Catalog` in SimpleDB) consists of a list of the tables and schemas of the tables that are currently in the database. You will need to support the ability to add a new table, as well as getting information about a particular table. Associated with each table is a `TupleDesc` object that allows operators to determine the types and number of fields in a table.（注意一个表只有一个TupleDesc对象实体，只不过一个表中的每个Tuple都有指针指向它）

The global catalog is a single instance of `Catalog` that is allocated for the entire SimpleDB process. The global catalog can be retrieved via the method `Database.getCatalog()`, and the same goes for the global buffer pool (using `Database.getBufferPool()`).

#### Interface:

##### DbFile

```java
/**
 * The interface for database files on disk. Each table is represented by a
 * single DbFile. DbFiles can fetch pages and iterate through tuples. Each
 * file has a unique id used to store metadata about the table in the Catalog.
 * DbFiles are generally accessed through the buffer pool, rather than directly
 * by operators.
 */
public interface DbFile {
    /**
     * Read the specified page from disk.
     *
     * @throws IllegalArgumentException if the page does not exist in this file.
     */
    public Page readPage(PageId id);

    /**
     * Push the specified page to disk.
     *
     * @param p The page to write.  page.getId().pageno() specifies the offset into the file where the page should be written.
     * @throws IOException if the write fails
     *
     */
    public void writePage(Page p) throws IOException;

    /**
     * Inserts the specified tuple to the file on behalf of transaction.
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     *
     * @param tid The transaction performing the update
     * @param t The tuple to add.  This tuple should be updated to reflect that
     *          it is now stored in this file.
     * @return An ArrayList contain the pages that were modified
     * @throws DbException if the tuple cannot be added
     * @throws IOException if the needed file can't be read/written
     */
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException;

    /**
     * Removes the specified tuple from the file on behalf of the specified
     * transaction.
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     *
     * @param tid The transaction performing the update
     * @param t The tuple to delete.  This tuple should be updated to reflect that
     *          it is no longer stored on any page.
     * @return An ArrayList contain the pages that were modified
     * @throws DbException if the tuple cannot be deleted or is not a member
     *   of the file
     */
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException;

    /**
     * Returns an iterator over all the tuples stored in this DbFile. The
     * iterator must use {@link BufferPool#getPage}, rather than
     * {@link #readPage} to iterate through the pages.
     *
     * @return an iterator over all the tuples stored in this DbFile.
     */
    public DbFileIterator iterator(TransactionId tid);

    /**
     * Returns a unique ID used to identify this DbFile in the Catalog. This id
     * can be used to look up the table via {@link Catalog#getDatabaseFile} and
     * {@link Catalog#getTupleDesc}.
     * <p>
     * Implementation note:  you will need to generate this tableid somewhere,
     * ensure that each HeapFile has a "unique id," and that you always
     * return the same value for a particular HeapFile. A simple implementation
     * is to use the hash code of the absolute path of the file underlying
     * the HeapFile, i.e. <code>f.getAbsoluteFile().hashCode()</code>.
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId();
    
    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc();
}
```

##### DbFileIterator

```java
/**
 * DbFileIterator is the iterator interface that all SimpleDB Dbfile should
 * implement.
 */
public interface DbFileIterator{
    /**
     * Opens the iterator
     * @throws DbException when there are problems opening/accessing the database.
     */
    public void open()
        throws DbException, TransactionAbortedException;

    /** @return true if there are more tuples available, false if no more tuples or iterator isn't open. */
    public boolean hasNext()
        throws DbException, TransactionAbortedException;

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the iterator.
     * @throws NoSuchElementException if there are no more tuples
     */
    public Tuple next()
        throws DbException, TransactionAbortedException, NoSuchElementException;

    /**
     * Resets the iterator to the start.
     * @throws DbException When rewind is unsupported.
     */
    public void rewind() throws DbException, TransactionAbortedException;

    /**
     * Closes the iterator.
     */
    public void close();
}
```

**Exercise 2.**

- src/simpledb/Catalog.java

  ```java
  /**
   * The Catalog keeps track of all available tables in the database and their
   * associated schemas.
   * For now, this is a stub catalog that must be populated with tables by a
   * user program before it can be used -- eventually, this should be converted
   * to a catalog that reads a catalog table from disk.
   * 
   * @Threadsafe
   */
  public class Catalog {
  	private HashMap<Integer, Table> tables;
  	private HashMap<String, Integer> name2ID;
  	
  	private class Table {
  		public String tableName;
  		public DbFile dbFile; // 一个Table与一个DbFile关联。
  		public String pkeyField;
  		
  		public Table(DbFile dbFile, String tableName, String pkeyField) {
  			this.dbFile = dbFile;
  			this.tableName = tableName;
  			this.pkeyField = pkeyField;
  		}
  	};
      /**
       * Add a new table to the catalog.
       * This table's contents are stored in the specified DbFile.
       * @param file the contents of the table to add;  file.getId() is the identfier of
       *    this file/tupledesc param for the calls getTupleDesc and getFile
       * @param name the name of the table -- may be an empty string.  May not be null.  If a name
       * conflict exists, use the last table to be added as the table for a given name.
       * @param pkeyField the name of the primary key field
       */
      public void addTable(DbFile file, String name, String pkeyField) {
          // some code goes here
      	if (name2ID.containsKey(name)) {
      		tables.remove(name2ID.get(name));
      		name2ID.replace(name, file.getId());
      	} else {
      		name2ID.put(name, file.getId());
      	}
  		tables.put(file.getId(), new Table(file, name, pkeyField));
      }
  
      public void addTable(DbFile file, String name) {
          addTable(file, name, "");
      }
  }
  ```
  
  通过测试CatalogTest。

### 2.4. BufferPool

The buffer pool (class `BufferPool` in SimpleDB) is **responsible for caching pages in memory that have been recently read from disk.** **All operators read and write pages from various files on disk through the buffer pool.（这是必须的，不然缓存就失去了意义，使得存取效率并没有得到提升，也使一致性难以维护）** It consists of a fixed number of pages, defined by the `numPages` parameter to the `BufferPool` constructor. In later labs, you will implement an eviction policy.

#### Interface:

##### Page

```java
/**
 * Page is the interface used to represent pages that are resident in the
 * BufferPool.  Typically, DbFiles will read and write pages from disk.
 * <p>
 * Pages may be "dirty", indicating that they have been modified since they
 * were last written out to disk.
 *
 * For recovery purposes, pages MUST have a single constructor of the form:
 *     Page(PageId id, byte[] data)
 */
public interface Page {

    /**
     * Return the id of this page.  The id is a unique identifier for a page
     * that can be used to look up the page on disk or determine if the page
     * is resident in the buffer pool.
     *
     * @return the id of this page
     */
    public PageId getId();

    /**
     * Get the id of the transaction that last dirtied this page, or null if the page is clean..
     *
     * @return The id of the transaction that last dirtied this page, or null
     */
    public TransactionId isDirty();

  /**
   * Set the dirty state of this page as dirtied by a particular transaction
   */
    public void markDirty(boolean dirty, TransactionId tid);

  /**
   * Generates a byte array representing the contents of this page.
   * Used to serialize this page to disk.
   * <p>
   * The invariant here is that it should be possible to pass the byte array
   * generated by getPageData to the Page constructor and have it produce
   * an identical Page object.
   *
   * @return A byte array correspond to the bytes of this page.
   */

    public byte[] getPageData();

    /** Provide a representation of this page before any modifications were made
        to it.  Used by recovery.
    */
    public Page getBeforeImage();

    /*
     * a transaction that wrote this page just committed it.
     * copy current content to the before image.
     */
    public void setBeforeImage();
}
```

DbFile是Page的集合，Page也是BufferPool缓存的单位。注意到开头的注释，从磁盘中读取Page和将Page写入磁盘都是由Page所属的DbFile负责的（分别是DbFile.readPage()和DbFile.writePage()），这是可以理解的，从概念上，DbFile是Page的集合，那么DbFile也应该负责存取Page。另一方面，如果不这么做，而是由BufferPool负责存取Page，那么会使得系统不太好扩展，至少如果我们要往系统中加入一个新的DbFile实现，那么我们就需要改动BufferPool的相关代码，添加类型分派分支。而有DbFile负责存取Page，那么当添加一个新的DbFile实现时，只需实现这个新的DbFile的存取Page的方法，而不需要改动BufferPool的任何代码。**这使得编程人员不需要了解系统其它部分的具体实现（如BufferPool的具体实现）即可对系统进行扩展。**

**Exercise 3.**

- src/simpledb/BufferPool.java

  ```java
  /**
   * BufferPool manages the reading and writing of pages into memory from
   * disk. Access methods call into it to retrieve pages, and it fetches
   * pages from the appropriate location.
   * <p>
   * The BufferPool is also responsible for locking;  when a transaction fetches
   * a page, BufferPool checks that the transaction has the appropriate
   * locks to read/write the page.
   * 
   * @Threadsafe, all fields are final
   */
  public class BufferPool {
  	private int numPages;
  	private HashMap<PageId, Page> pages;
  	
      /** Bytes per page, including header. */
      private static final int DEFAULT_PAGE_SIZE = 4096;
  
      private static int pageSize = DEFAULT_PAGE_SIZE;
      
      /** Default number of pages passed to the constructor. This is used by
      other classes. BufferPool should use the numPages argument to the
      constructor instead. */
      public static final int DEFAULT_PAGES = 50;
      
      /**
       * Retrieve the specified page with the associated permissions.
       * Will acquire a lock and may block if that lock is held by another
       * transaction.
       * <p>
       * The retrieved page should be looked up in the buffer pool.  If it
       * is present, it should be returned.  If it is not present, it should
       * be added to the buffer pool and returned.  If there is insufficient
       * space in the buffer pool, a page should be evicted and the new page
       * should be added in its place.
       *
       * @param tid the ID of the transaction requesting the page
       * @param pid the ID of the requested page
       * @param perm the requested permissions on the page
       */
      public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
          throws TransactionAbortedException, DbException {
          // some code goes here
      	if (pages.size() > numPages)
      		throw new DbException(null);
      	if (pages.containsKey(pid)) { // cache hit.
      		return pages.get(pid);
      	} else { // cache miss.
      		DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
      		// XXX 面向接口编程，即使我还没实现DbFile.readPage()，但我知道它的行为、它的输入输出(不需要关心具体实现，如怎么定位到指定的Page，而且不同的具体类型实现也不同)，那么我就可以直接编写出这部分代码。
      		Page page = dbFile.readPage(pid);
      		pages.put(pid, page);
      		return page;
      	}
      }
  ```

### 2.5. HeapFile access method

BufferPool在内存中缓存Page，HeapFile是HeapPage的集合，HeapPage是tuple的集合，HeapPage首先有一个header，这是一个bitmap，故每一个tuple在HeapPage中占用的bit数为`tuple size * 8 + 1`。从磁盘中读写Page是由DbFile的readPage()负责的，其它对象读写Page都是通过BufferPool的getPage()来获取，getPage()如果缓存miss，则委托DbFile的readPage()从磁盘读入Page。

`_tuples per page_ = floor((_page size_ * 8) / (_tuple size_ * 8 + 1))`

The floor operation rounds down to the nearest integer number of tuples (we don't want to store partial tuples on a page!)

`headerBytes = ceiling(tupsPerPage/8)`

The ceiling operation rounds up to the nearest integer number of bytes (we never store less than a full byte of header information.)

Also, note that the high-order bits of the last byte may not correspond to a slot that is actually in the file, since the number of slots may not be a multiple of 8. Also note that all Java virtual machines are big-endian.（即内存地址递增的方向，大端序会先存放最高有效字节，小端序会先存放最低有效字节）

**Exercise 4.**

- src/simpledb/HeapPageId.java

  ```java
  public class HeapPageId implements PageId {
  	int tableId;
  	int pgNo;
  }
  ```

  通过测试HeapPageIdTest。

- src/simpledb/RecordID.java

  ```java
  public class RecordId implements Serializable {
  	private PageId pId;
  	private int tupleNo;
  }
  ```

  通过测试RecordIdTest。

- src/simpledb/HeapPage.java

  ```java
  public class HeapPage implements Page {
  
      final HeapPageId pid;
      final TupleDesc td;
      final byte header[];
      final Tuple tuples[];
      final int numSlots;
      
      public HeapPage(HeapPageId id, byte[] data) throws IOException {
          this.pid = id;
          this.td = Database.getCatalog().getTupleDesc(id.getTableId());
          this.numSlots = getNumTuples();
          DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
  
          // allocate and read the header slots of this page
          header = new byte[getHeaderSize()];
          for (int i=0; i<header.length; i++)
              header[i] = dis.readByte();
          
          tuples = new Tuple[numSlots];
          try{
              // allocate and read the actual records of this page
              for (int i=0; i<tuples.length; i++)
                  tuples[i] = readNextTuple(dis,i); // tuples[i]可能指向null，如果slotId为i的Tuple在该Page中不存在的话。
          }catch(NoSuchElementException e){
              e.printStackTrace();
          }
          dis.close();
  
          setBeforeImage();
      }
      
      private int getNumTuples() {
          // some code goes here
      	return (int) Math.floor(BufferPool.getPageSize()*8/(td.getSize()*8+1));
      }
      
      private int getHeaderSize() {        
          // some code goes here
          return (int) Math.ceil(numSlots/8.0); // XXX 注意这里要做浮点数除法，而不是整数除法，如果写成了整数除法，那么除非刚好整除，否则就会数组下标访问越界。
      }
      
      public boolean isSlotUsed(int i) {
          // some code goes here
          return (header[i/8]&(1<<(i%8)))!=0;
      }
      
      public Iterator<Tuple> iterator() {
          // some code goes here
          return new Iterator<Tuple>() {
          	int i = 0;
          	
  			@Override
  			public Tuple next() {
  				Tuple tuple = tuples[i];
  				i++;
  				return tuple;
  			}
  			
  			@Override
  			public boolean hasNext() {
  				while (i<numSlots && !isSlotUsed(i)) // 注意要判断一下i，避免越界。
  					i++;
  				return i<numSlots;
  			}
  		};
      }
  }
  ```

  通过测试HeapPageReadTest。

**Exercise 5.**

- src/simpledb/HeapFile.java

  ```java
  public class HeapFile implements DbFile {
  	private File file;
  	private TupleDesc tDesc;
  	private int numPages;
  	private int tableId;
      
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
      
      public DbFileIterator iterator(TransactionId tid) {
          // some code goes here
          return new DbFileIterator() {
          	boolean opened = false;
          	// boolean closed = false; // 这个closed是多余的，因为一个迭代器要么已开启，要么已关闭。
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
  				// if (!opened || closed)
                  if (!opened)
  					throw new NoSuchElementException();
  				// 这里假定caller每次都会先检查hasNext()再调用next()，所以没有做更多的检查。
  				return it.next();
  			}
  			
  			@Override
  			public boolean hasNext() throws DbException, TransactionAbortedException {
  				// TODO Auto-generated method stub
  				// if (!opened || closed)
                  if (!opened)
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
  				// closed = true;
                  opened = false;
  			}
  		};
    }
  }
  ```
  
  通过测试HeapFileReadTest。

### 2.6. Operators

**Operators are responsible for the actual execution of the query plan. They implement the operations of the relational algebra. In SimpleDB, operators are iterator based**; each operator implements the `DbIterator` interface.

**At the top of the plan, the program interacting with SimpleDB simply calls `getNext` on the root operator; this operator then calls `getNext` on its children, and so on, until these leaf operators are called. They fetch tuples from disk and pass them up the tree (as return arguments to `getNext`); tuples propagate up the plan in this way until they are output at the root or combined or rejected by another operator in the plan.**

**Exercise 6.**

- src/simpledb/SeqScan.java

  ```java
  public class SeqScan implements OpIterator {
  	private TransactionId tId;
  	private int tableId;
  	private String tableAlias;
  	private DbFile dbFile;
  	private DbFileIterator iterator;
      
      public void open() throws DbException, TransactionAbortedException {
          // some code goes here
      	dbFile = Database.getCatalog().getDatabaseFile(tableId);
      	iterator = dbFile.iterator(tId);
      	iterator.open();
      }
      
      public TupleDesc getTupleDesc() {
          // some code goes here
      	// 没有检查this.dbFile是否不为null，即caller是否先调用了open()。
          TupleDesc tupleDesc = dbFile.getTupleDesc();
          Type[] typeAr = new Type[tupleDesc.numFields()];
          String[] fieldAr = new String[tupleDesc.numFields()];
          for (int i = 0; i < tupleDesc.numFields(); i++) {
          	typeAr[i] = tupleDesc.getFieldType(i);
  			fieldAr[i] = tableAlias + "." + tupleDesc.getFieldName(i);
  		}
  		return new TupleDesc(typeAr, fieldAr);
      }
      
      public boolean hasNext() throws TransactionAbortedException, DbException {
          // some code goes here
      	if (dbFile==null || iterator==null)
      		throw new IllegalStateException();
          return iterator.hasNext();
      }
  
      public Tuple next() throws NoSuchElementException,
              TransactionAbortedException, DbException {
          // some code goes here
      	if (dbFile==null || iterator==null)
      		throw new IllegalStateException();
          return iterator.next();
      }
  
      public void close() {
          // some code goes here
      	iterator.close();
      }
  
      public void rewind() throws DbException, NoSuchElementException,
              TransactionAbortedException {
          // some code goes here
      	iterator.rewind();
      }
  }
  ```

  通过测试ScanTest。

### 2.7. A simple query

```java
package simpledb;

import java.io.File;

public class TestLab1 {
    public static void main(String[] argv) {

        // construct a 3-column table schema
        Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
        String names[] = new String[]{ "field0", "field1", "field2" };
        TupleDesc descriptor = new TupleDesc(types, names);

        // create the table, associate it with some_data_file.dat
        // and tell the catalog about the schema of this table.
        HeapFile table1 = new HeapFile(new File("lab1.dat"), descriptor); // 当前目录并不是该文件所在的目录，而是项目的根目录。
        Database.getCatalog().addTable(table1, "test");

        // construct the query: we use a simple SeqScan, which spoonfeeds
        // tuples via its iterator.
        TransactionId tid = new TransactionId();
        SeqScan f = new SeqScan(tid, table1.getId());
        try {
            // and run it
            f.open();
            while (f.hasNext()) {
                Tuple tup = f.next();
                System.out.println(tup);
            }
            f.close();
            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            System.out.println ("Exception : " + e);
        }
    }
}
```

