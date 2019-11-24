package simpledb;

import java.io.File;
import java.io.Serializable;

import simpledb.Predicate.Op;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {
	private int fieldNo1;
	private int fieldNo2;
	private Op op;

    private static final long serialVersionUID = 1L;

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * @param field1
     *            The field index into the first tuple in the predicate
     * @param field2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        // some code goes here
    	this.fieldNo1 = field1;
    	this.op = op;
    	this.fieldNo2 = field2;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        // some code goes here
        return t1.getField(fieldNo1).compare(op, t2.getField(fieldNo2)); // 比较两个字段值，这是Field应该提供的，确实现有代码已经提供了，那么只需简单调用即可/把比较的工作委托下去即可。
    }
    
    public int getField1()
    {
        // some code goes here
        return fieldNo1;
    }
    
    public int getField2()
    {
        // some code goes here
        return fieldNo2;
    }
    
    public Predicate.Op getOperator()
    {
        // some code goes here
        return op;
    }
}
