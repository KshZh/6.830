# 6.830 Lab 2: SimpleDB Operators

Note that SimpleDB does not implement any kind of consistency or integrity checking, so it is possible to insert duplicate records into a file and there is no way to enforce（实施、执行、强制） primary or foreign key constraints.

Finally, you might notice that the iterators in this lab extend the Operator class instead of implementing the OpIterator interface. Because the implementation of next/hasNext is often repetitive, annoying, and error-prone, Operator implements this logic generically, and only requires that you implement a simpler readNext.

迭代器模式：提供一种方法访问一个聚合对象中的各个元素，而又不需要暴露该对象的内部表示，且为遍历不同的聚合结构提供了一个统一的接口。

```java
/**
 * OpIterator is the iterator interface that all SimpleDB operators should
 * implement. If the iterator is not open, none of the methods should work,
 * and should throw an IllegalStateException.  In addition to any
 * resource allocation/deallocation, an open method should call any
 * child iterator open methods, and in a close method, an iterator
 * should call its children's close methods.
 */
public interface OpIterator extends Serializable{
  /**
   * Opens the iterator. This must be called before any of the other methods.
   * @throws DbException when there are problems opening/accessing the database.
   */
  public void open()
      throws DbException, TransactionAbortedException;

  /** Returns true if the iterator has more tuples.
   * @return true f the iterator has more tuples.
   * @throws IllegalStateException If the iterator has not been opened
 */
  public boolean hasNext() throws DbException, TransactionAbortedException;

  /**
   * Returns the next tuple from the operator (typically implementing by reading
   * from a child operator or an access method).
   *
   * @return the next tuple in the iteration.
   * @throws NoSuchElementException if there are no more tuples.
   * @throws IllegalStateException If the iterator has not been opened
   */
  public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException;

  /**
   * Resets the iterator to the start.
   * @throws DbException when rewind is unsupported.
   * @throws IllegalStateException If the iterator has not been opened
   */
  public void rewind() throws DbException, TransactionAbortedException;

  /**
   * Returns the TupleDesc associated with this OpIterator.
   * @return the TupleDesc associated with this OpIterator.
   */
  public TupleDesc getTupleDesc();

  /**
   * Closes the iterator. When the iterator is closed, calling next(),
   * hasNext(), or rewind() should fail by throwing IllegalStateException.
   */
  public void close();
}
```

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
          // some code goes here
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
          // some code goes here
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
          // some code goes here
      	// 别忘了，Filter也是一个Operator，也是一个OpIterator。
      	// 这里要明确指出调用的是父类的open()，而不是子类的open()，后者会无限递归。
      	super.open();
      	childIterator.open();
      }
      
      protected Tuple fetchNext() throws NoSuchElementException,
              TransactionAbortedException, DbException {
          // some code goes here
      	Tuple tuple;
      	// 这就是接口的强大之处，约定的接口，约定的行为、输入输出，不需要知道具体类型，是SeqScan还是别的，无所谓，反正迭代这个可迭代对象可以得到Tuple就够了。约定的接口使得我们可以很容易给系统加入新的组件，而不需要改动现有代码。
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

注意StringAggregator和IntegerAggregator中的类型前缀，指的是要进行聚合的字段的类型，是StringField还是IntField，而group的字段的类型在两个类中都可以是StringField或IntField。

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
  
  // 定义在接口Aggregator中，注意确保_DUMMY_FIELD的值不会被用户用到。
  static final Field _DUMMY_FIELD = new StringField("", 0);
  ```

- src/simpledb/StringAggregator.java

- src/simpledb/Aggregate.java

  ```java
  public class Aggregate extends Operator {
  	private OpIterator childIterator;
  	private Aggregator aggregator;
  	private int gbField;
  	private int aField;
  	private TupleDesc tDesc;
  	private Op op;
      
      public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
      	// some code goes here
      	this.childIterator = child;
      	this.gbField = gfield;
      	this.aField = afield;
      	this.op = aop;
      	Type gType = null;
      	TupleDesc tDesc = child.getTupleDesc();
      	Type aType = tDesc.getFieldType(afield);
      	if (gfield != -1) {
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
  }
  ```

### 2.3. HeapFile Mutability

