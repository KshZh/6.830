package simpledb;

import java.text.ParseException;
import java.io.*;

/**
 * Class representing a type in SimpleDB.
 * Types are static objects defined by this class; hence, the Type
 * constructor is private.
 */
public enum Type implements Serializable {
    // XXX 类型，并不存储实际的数据。但类型可以负责解析属于该类型的数据/对象。
	// 同样，每个Type要自己根据数据的组织方式实现parse()，这样给系统扩展新的Type时才不需要修改其它部分代码。
    // XXX 在SimpleDB中，从语言层面上看，字段值Field对象的类型当然是Field，但从更高级的抽象层次看，字段值Field对象的类型是schema/TupleDesc中描述的字段类型/域，即Type。
	// 具体地，IntField的类型是INT_TYPE，StringField的类型是STRING_TYPE。
    INT_TYPE() {
        @Override
        public int getLen() {
            return 4;
        }

        @Override
        public Field parse(DataInputStream dis) throws ParseException {
            try {
                return new IntField(dis.readInt());
            }  catch (IOException e) {
                throw new ParseException("couldn't parse", 0);
            }
        }

    }, STRING_TYPE() {
        @Override
        public int getLen() {
            return STRING_LEN+4;
        }

        @Override
        public Field parse(DataInputStream dis) throws ParseException {
            try {
                int strLen = dis.readInt();
                byte bs[] = new byte[strLen];
                dis.read(bs);
                dis.skipBytes(STRING_LEN-strLen);
                return new StringField(new String(bs), STRING_LEN);
            } catch (IOException e) {
                throw new ParseException("couldn't parse", 0);
            }
        }
    };
    
    public static final int STRING_LEN = 128;

  /**
   * @return the number of bytes required to store a field of this type.
   */
    public abstract int getLen();

  /**
   * @return a Field object of the same type as this object that has contents
   *   read from the specified DataInputStream.
   * @param dis The input stream to read from
   * @throws ParseException if the data read from the input stream is not
   *   of the appropriate type.
   */
    public abstract Field parse(DataInputStream dis) throws ParseException;

}
