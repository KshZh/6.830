# 6.830 Lab 2: SimpleDB Operators

Note that SimpleDB does not implement any kind of consistency or integrity checking, so it is possible to insert duplicate records into a file and there is no way to enforce（实施、执行、强制） primary or foreign key constraints.

Finally, you might notice that the iterators in this lab extend the Operator class instead of implementing the OpIterator interface. Because the implementation of next/hasNext is often repetitive, annoying, and error-prone, Operator implements this logic generically, and only requires that you implement a simpler readNext.

```java
// 提取了一个较为通用的迭代模式。
// 当然，对于不满足这个迭代模式的operator，则要自己实现OpIterator接口。
public abstract class Operator implements OpIterator {
    private Tuple next = null;
    private boolean open = false;
    private int estimatedCardinality = 0;
    
    private static final long serialVersionUID = 1L;
    
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (!this.open)
            throw new IllegalStateException("Operator not yet open");
        
        if (next == null)
            next = fetchNext();
        return next != null;
    }

    public Tuple next() throws DbException, TransactionAbortedException,
            NoSuchElementException {
        if (next == null) {
            next = fetchNext();
            if (next == null)
                throw new NoSuchElementException();
        }

        Tuple result = next;
        next = null;
        return result;
    }

    public void open() throws DbException, TransactionAbortedException {
        this.open = true;
    }
    
    public void close() {
        // Ensures that a future call to next() will fail
        next = null;
        this.open = false;
    }
    
    // 抽象方法，待子类实现。
    // protected，在继承树中可见。
    protected abstract Tuple fetchNext() throws DbException,
            TransactionAbortedException;
}
```

## 2. SimpleDB Architecture and Implementation Guide

### 2.1. Filter and Join

**Exercise 1.**

- src/simpledb/Predicate.java

  ```java
  public class Predicate implements Serializable {
  	private int fieldNo; // 要比较的字段在TupleDesc中的序号。
  	private Op op;
  	private Field operand;
      
      public boolean filter(Tuple t) {
      	return t.getField(fieldNo).compare(op, operand); // 这里operand是作为第二个操作数。
      }
  }
  ```
  

通过测试PredicateTest。

- src/simpledb/JoinPredicate.java

  ```java
  // Predicate预先接收一个Tuple，它的filter()再接收一个Tuple。JoinPredicate的filter()则一次接收两个Tuple。
  public class JoinPredicate implements Serializable {
  	private int fieldNo1;
  	private int fieldNo2;
  	private Op op;
      
      public boolean filter(Tuple t1, Tuple t2) {
          return t1.getField(fieldNo1).compare(op, t2.getField(fieldNo2)); // 比较两个字段值，这是Field应该提供的，确实现有代码已经提供了，那么只需简单调用即可/把比较的工作委托下去即可。
      }
  }
  ```
  

通过测试JoinPredicateTest。

- src/simpledb/Filter.java

  **约定的接口使得我们可以很容易给系统加入新的组件，而不需要改动现有代码。**可以看到，在SimpleDB中制定了OpIterator接口后，添加新的operator是很容易的，不需要改动任何现有代码，同理，制定了Field接口后，添加新的字段类型（如浮点数、时间戳等）也是不需要改动现有代码的，这样的例子在SimpleDB中随处可见。

  那么**一个重要的技能就是，设计合理的接口**。比如Field接口，如果该接口没有定义按特定操作比较两个Field的compare()接口的话，那么我们只能把该操作写在如上面的Predicate和JoinPredicate的filter()中，那么当我们添加新的字段类型时，显然就要修改这两个类的filter()，添加新的类型分派分支。

  ```java
  public class Filter extends Operator {
  	private Predicate predicate;
  	private OpIterator childIterator;
      
      public void open() throws DbException, NoSuchElementException,
              TransactionAbortedException {
      	// 别忘了，Filter也是一个Operator，也是一个OpIterator。
      	// 这里要明确指出调用的是父类的open()，而不是子类的open()，后者会无限递归。
      	super.open();
      	childIterator.open();
      }
      
      protected Tuple fetchNext() throws NoSuchElementException,
              TransactionAbortedException, DbException {
      	Tuple tuple;
      	// 这就是接口的强大之处，约定的接口，约定的行为、输入输出，不需要知道具体类型、具体实现，是SeqScan还是别的，无所谓，反正知道迭代这个可迭代对象可以得到Tuple就够了。约定的接口使得我们可以很容易给系统加入新的组件，而不需要改动现有代码。
      	while (childIterator.hasNext()) {
      		tuple = childIterator.next();
      		if (predicate.filter(tuple)) {
      			return tuple;
      		}
      	}
          return null;
      }
  }
  ```
  
  通过测试FilterTest和system test中的FilterTest。

- src/simpledb/Join.java

  ```java
  public class Join extends Operator {
  	private JoinPredicate joinPredicate; // JoinPredicate中已经包含了要比较的两个Tuple的两个字段的编号。
  	private OpIterator childIterator1;
  	private OpIterator childIterator2;
  	private TupleDesc tupleDesc;
  	private Tuple pendingTuple1;
  	private Tuple pendingTuple2;
      
      protected Tuple fetchNext() throws TransactionAbortedException, DbException {
          // some code goes here
      	// 注意，join做的操作是笛卡尔积，而不是简单地把行号相等的行连接起来。
          // 最简单粗暴的嵌套循环算法。
      	Tuple tuple = new Tuple(tupleDesc);
      	Tuple tuple1, tuple2;
      	while (childIterator1.hasNext() || pendingTuple1!=null) {
      		if (pendingTuple1 != null) {
      			tuple1 = pendingTuple1;
      			pendingTuple1 = null;
      		} else {
      			tuple1 = childIterator1.next();	
      		}
      		if (pendingTuple2 == null) {
      			childIterator2.rewind();
      		}
      		while (childIterator2.hasNext() || pendingTuple2!=null) {
      			if (pendingTuple2 != null) {
      				tuple2 = pendingTuple2;
      				pendingTuple2 = null;
      			} else {
      				tuple2 = childIterator2.next();
      			}
  	    		if (joinPredicate.filter(tuple1, tuple2)) {
  	    			int n = 0;
  	    			for (int i = 0; i < tuple1.getTupleDesc().numFields(); i++, n++) {
  						tuple.setField(n, tuple1.getField(i));
  					}
  	    			for (int i = 0; i < tuple2.getTupleDesc().numFields(); i++, n++) {
  						tuple.setField(n, tuple2.getField(i));
  					}
  	    			if (childIterator2.hasNext()) {
  	    				 pendingTuple2 = childIterator2.next();
  	    				 pendingTuple1 = tuple1;
  	    			} else {
  	    				// pendingTuple1和pendingTuple2继续保持为null。
  	    			}
  	    			return tuple;
  	    		}
      		}
      	}
          return null;
      }
  }
  ```

  通过测试JoinTest和system test中的JoinTest。

### 2.2. Aggregates

注意StringAggregator和IntegerAggregator中的类型前缀，指的是要进行聚合的字段值的类型，是StringField还是IntField，而group by的字段的类型在两个类中都可以是StringField或IntField。这点可以从构造函数的参数看出：`IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what)`。

#### Interface:

##### Aggregator

```java
/**
 * The common interface for any class that can compute an aggregate over a
 * list of Tuples.
 */
public interface Aggregator extends Serializable {
    static final int NO_GROUPING = -1;

    static final Field _DUMMY_FIELD = new StringField("", 0);
    
    /**
     * SUM_COUNT and SC_AVG will
     * only be used in lab7, you are not required
     * to implement them until then.
     * */
    public enum Op implements Serializable {
        MIN, MAX, SUM, AVG, COUNT,
        /**
         * SUM_COUNT: compute sum and count simultaneously, will be
         * needed to compute distributed avg in lab7.
         * */
        SUM_COUNT,
        /**
         * SC_AVG: compute the avg of a set of SUM_COUNT tuples,
         * will be used to compute distributed avg in lab7.
         * */
        SC_AVG;

        /**
         * Interface to access operations by a string containing an integer
         * index for command-line convenience.
         *
         * @param s a string containing a valid integer Op index
         */
        public static Op getOp(String s) {
            return getOp(Integer.parseInt(s));
        }

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         *
         * @param i a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }
        
        public String toString()
        {
        	if (this==MIN)
        		return "min";
        	if (this==MAX)
        		return "max";
        	if (this==SUM)
        		return "sum";
        	if (this==SUM_COUNT)
    			return "sum_count";
        	if (this==AVG)
        		return "avg";
        	if (this==COUNT)
        		return "count";
        	if (this==SC_AVG)
    			return "sc_avg";
        	throw new IllegalStateException("impossible to reach here");
        }
    }

    /**
     * Merge a new tuple into the aggregate for a distinct group value;
     * creates a new group aggregate result if the group value has not yet
     * been encountered.
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup);

    /**
     * Create a OpIterator over group aggregate results.
     * @see simpledb.TupleIterator for a possible helper
     */
    public OpIterator iterator();
    
}
```

**Exercise 2.**

- src/simpledb/IntegerAggregator.java

  ```java
  // 错误的做法！这样代码很复杂，有很多分支判断。
  // private HashMap<Integer, Integer> resultOfGroupsOfInteger;
  // private HashMap<String, Integer> resultOfGroupsOfString;
  // private HashMap<String, Integer> numTuplePerGroupOfString;
  // private HashMap<Integer, Integer> numTuplePerGroupOfInteger;
  // 要让我们的代码更泛用，就要提高类型的抽象层级。
  private HashMap<Field, Integer> resultOfGroups;
  private HashMap<Field, Integer> numTuplePerGroup;
  private int resultOfNoGrouping;
  private int n;
  
  // 更进一步，可以制作一个DUMMY_FIELD，然后数据成员可以进一步精简为：
  private HashMap<Field, Integer> resultOfGroups;
  private HashMap<Field, Integer> numTuplePerGroup;
  // 而且代码又少了一些分支，更简洁。
  
  // 定义在接口Aggregator中，注意确保_DUMMY_FIELD的值不会被客户用到。
  static final Field _DUMMY_FIELD = new StringField("", 0);
  ```

  ```java
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
      
      public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
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
  ```

  通过测试IntegerAggregatorTest。

- src/simpledb/StringAggregator.java

  类似于IntegerAggregator，只不过StringAggregator只支持COUNT操作，因为其它聚合操作对StringField没有意义。

  通过测试StringAggregatorTest。

- src/simpledb/Aggregate.java

  ```java
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
      
      protected Tuple fetchNext() throws TransactionAbortedException, DbException {
  		// some code goes here
      	if (childIterator.hasNext()) {
      		return childIterator.next();
      	}
  		return null;
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
  ```
  
  通过测试AggregateTest和system test中的AggregateTest。

### 2.3. HeapFile Mutability

**Exercise 3.**

- src/simpledb/HeapPage.java

  ```java
  /**
   * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
   *   that it is no longer stored on any page.
   * @throws DbException if this tuple is not on this page, or tuple slot is
   *         already empty.
   * @param t The tuple to delete
   */
  public void deleteTuple(Tuple t) throws DbException {
      // some code goes here
      // not necessary for lab1
  	int tupleNo = t.getRecordId().getTupleNumber();
  	if (t.getRecordId().getPageId()!=pid || tupleNo >= numSlots || !isSlotUsed(tupleNo)) {
  		throw new DbException("Tuple is not on this page, or tuple slot is already empty.");
  	}
  	markSlotUsed(tupleNo, false); // 只需要标记一下bitmap即可，不需要把对应的slot清零。
  }
  
  /**
   * Adds the specified tuple to the page;  the tuple should be updated to reflect
   *  that it is now stored on this page.
   * @throws DbException if the page is full (no empty slots) or tupledesc
   *         is mismatch.
   * @param t The tuple to add.
   */
  public void insertTuple(Tuple t) throws DbException {
      // some code goes here
      // not necessary for lab1
  	for (int i = 0; i < numSlots; i++) { // 不要用`i<header.length*8`，因为header的末尾几个位可能没被使用但却为0。
  		if (!isSlotUsed(i)) {
  			tuples[i] = t;
  			t.setRecordId(new RecordId(pid, i));
  			markSlotUsed(i, true);
  			return;
  		}
  	}
  	throw new DbException("The page is full.");
  }
  
  /**
   * Marks this page as dirty/not dirty and record that transaction
   * that did the dirtying
   */
  public void markDirty(boolean dirty, TransactionId tid) {
      // some code goes here
  	// not necessary for lab1
      // dirty = dirty; // XXX 成员变量与形参重名，必须使用this，否则就变成了两边都是引用的是形参。
  	this.dirty = dirty;
  	if (dirty) {
  		lastDirtiedThePage = tid;
  	} else {
  		lastDirtiedThePage = null;
  	}
  }
  
  /**
   * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
   */
  public TransactionId isDirty() {
      // some code goes here
  	// Not necessary for lab1
      return lastDirtiedThePage;      
  }
  
  /**
   * Returns the number of empty slots on this page.
   */
  public int getNumEmptySlots() {
      // some code goes here
  	// 检查bitmap即可。
  	int n = 0, i;
  	for (i = 0; i < numSlots; i++) {
  		if ((header[i/8]&(1<<(i%8))) == 0) {
  			n++;
  		}
  	}
      return n;
  }
  
  /**
   * Returns true if associated slot on this page is filled.
   */
  public boolean isSlotUsed(int i) {
      // some code goes here
      return (header[i/8]&(1<<(i%8)))!=0;
  }
  
  /**
   * Abstraction to fill or clear a slot on this page.
   */
  private void markSlotUsed(int i, boolean value) {
      // some code goes here
      // not necessary for lab1
  	if (value) {
  		header[i/8] |= (1<<(i%8));
  	} else {
  		header[i/8] &= ~(1<<(i%8));
  	}
  }
  ```

  通过测试HeapPageWriteTest。

- src/simpledb/HeapFile.java
  (Note that you do not necessarily need to implement writePage at this point).

  ```java
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
      // 新建一个Page，并写入file中。
      // 这里似乎不能从BufferPool获取，因为其也是调用DbFile的readPage()来载入相应的Page，但如果DbFile的File中不存在这个Page的话，可能就越界了。
      // HeapPage page = (HeapPage) bufferPool.getPage(tid, new HeapPageId(tableID, numPages()), null);
      HeapPage page = new HeapPage(new HeapPageId(tableID, numPages()), HeapPage.createEmptyPageData());
      page.insertTuple(t);
      arrayList.add(page);
      writePage(page);
      return arrayList;
  }
  
  // see DbFile.java for javadocs
  public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
          TransactionAbortedException {
      // some code goes here
  	BufferPool bufferPool = Database.getBufferPool();
  	HeapPage page = (HeapPage)bufferPool.getPage(tid, t.getRecordId().getPageId(), null);
  	page.deleteTuple(t);
  	ArrayList<Page> arrayList = new ArrayList<Page>();
  	arrayList.add(page);
      return arrayList;
  }
  
  // see DbFile.java for javadocs
  public void writePage(Page page) throws IOException {
      // some code goes here
      // not necessary for lab1
  	int pgNo = page.getId().getPageNumber();
  	if (pgNo>=0 && pgNo<=numPages()) {
  		RandomAccessFile raf = new RandomAccessFile(file, "rw");
  		raf.seek(BufferPool.getPageSize()*numPages());
  		raf.write(page.getPageData(), 0, BufferPool.getPageSize());
  		raf.close();
  	} else {
      	throw new IllegalArgumentException("PageNo out of range");	
  	}
  }
  ```
  
通过测试HeapFileWriteTest。
  
- src/simpledb/BufferPool.java

  ```java
  public void insertTuple(TransactionId tid, int tableId, Tuple t)
      throws DbException, IOException, TransactionAbortedException {
      // some code goes here
      // not necessary for lab1
      ArrayList<Page> list = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
      for (Page page: list) {
          page.markDirty(true, tid);
  		pages.put(page.getId(), page);
      }
  }
  
  public  void deleteTuple(TransactionId tid, Tuple t)
      throws DbException, IOException, TransactionAbortedException {
      // some code goes here
      // not necessary for lab1
      ArrayList<Page> list = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId()).deleteTuple(tid, t);
      for (Page page: list) {
          page.markDirty(true, tid);
      }
  }
  ```

  通过测试BufferPoolWriteTest，这个测试有个坑，TODO。

### 2.4. Insertion and deletion

**Exercise 4.**

- src/simpledb/Insert.java

  ```java
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
  }
  ```

  通过测试InsertTest和同名system test。

- src/simpledb/Delete.java

  ```java
  /**
   * The delete operator. Delete reads tuples from its child operator and removes
   * them from the table they belong to.
   */
  public class Delete extends Operator {
  	private OpIterator child;
  	private TransactionId tID;
  	private TupleDesc tDesc;
  	private boolean fetchNextCalled;
      
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
  }
  ```

  通过system test。

### 2.5. Page eviction

**Exercise 5.**

