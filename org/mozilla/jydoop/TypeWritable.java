/* -*- Mode: Java; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

package org.mozilla.jydoop;

import org.python.core.PyObject;
import org.python.core.PyInteger;
import org.python.core.PyLong;
import org.python.core.PyFloat;
import org.python.core.PyString;
import org.python.core.PyTuple;
import org.python.core.Py;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.lang.AssertionError;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * This class is a Hadoop Writable which is capable of reflecting a defined subset of Python
 * objects as hadoop keys or values.
 *
 * The python types representable by this interface are:
 * - None
 * - int/long
 * - float (java double)
 * - string
 * - and tuples of the above types
 */
public class TypeWritable implements WritableComparable
{
  // The numeric value of these TYPE_ markers define the sort order
  // of TypeWritable instances
  static final byte TYPE_NONE = 0;
  static final byte TYPE_INT = 1;
  static final byte TYPE_FLOAT = 2;
  static final byte TYPE_STRING = 3;
  static final byte TYPE_TUPLE = 4;

  public PyObject value;

  private static void CheckType(PyObject obj)
  {
    if (obj == Py.None) {
      return;
    }
    if (obj instanceof PyInteger ||
        obj instanceof PyLong ||
        obj instanceof PyFloat ||
        obj instanceof PyString) {
      return;
    }
    if (obj instanceof PyTuple) {
      for (PyObject inner : ((PyTuple)obj).getArray()) {
        CheckType(inner);
      }
      return;
    }

    throw Py.TypeError("Expected int/float/str/tuple");
  }

  public TypeWritable()
  {
  }

  public TypeWritable(PyObject obj)
  {
    CheckType(obj);
    value = obj;
  }

  private static byte getType(PyObject obj)
  {
    if (obj == Py.None) {
      return TYPE_NONE;
    }
    if (obj instanceof PyInteger || obj instanceof PyLong) {
      return TYPE_INT;
    }
    if (obj instanceof PyFloat) {
      return TYPE_FLOAT;
    }
    if (obj instanceof PyString) {
      return TYPE_STRING;
    }
    if (obj instanceof PyTuple) {
      return TYPE_TUPLE;
    }
    throw new AssertionError("Unexpected type");
  }

  private static void WriteType(DataOutput out, PyObject obj) throws IOException
  {
    byte type = getType(obj);
    out.writeByte(type);
    switch (type) {
    case TYPE_NONE:
      return;

    case TYPE_INT:
      WritableUtils.writeVLong(out, obj.asLong());
      return;

    case TYPE_FLOAT:
      out.writeDouble(obj.asDouble());
      return;

    case TYPE_STRING:
      // DataOutput.writeUTF has a 64k limit. We exceed that with telemetry data, so
      // we do our own thing...
      byte[] b = obj.asString().getBytes("UTF-8");
      WritableUtils.writeVInt(out, b.length);
      out.write(b);
      return;

    case TYPE_TUPLE:
      WritableUtils.writeVInt(out, obj.__len__());
      for (int i = 0; i < obj.__len__(); ++i) {
        WriteType(out, obj.__getitem__(i));
      }
    }
  }

  public void write(DataOutput out) throws IOException
  {
    try {
      WriteType(out, value);
    }
    catch (Exception e) {
      Py.printException(e);
    }
  }


  private static PyObject ReadObject(DataInput in) throws IOException
  {
    byte type = in.readByte();
    switch (type) {
    case TYPE_NONE:
      return Py.None;
    case TYPE_INT: {
      long v = WritableUtils.readVLong(in);
      if (v > Integer.MAX_VALUE || v < Integer.MIN_VALUE) {
        return new PyLong(v);
      }
      else {
        return new PyInteger((int) v);
      }
    }
    case TYPE_FLOAT:
      return new PyFloat(in.readDouble());
    case TYPE_STRING:
      int blen = WritableUtils.readVInt(in);
      byte[] bytes = new byte[blen];
      in.readFully(bytes);
      return new PyString(new String(bytes, "UTF-8"));
    case TYPE_TUPLE:
      int l = WritableUtils.readVInt(in);
      PyObject[] objs = new PyObject[l];
      for (int i = 0; i < l; ++i) {
        objs[i] = ReadObject(in);
      }
      return new PyTuple(objs);
    }
    throw new AssertionError("Unexpected type value");
  }

  public void readFields(DataInput in) throws IOException
  {
    value = ReadObject(in);
  }

  private static byte getCompareType(byte type)
  {
    if (type == TYPE_FLOAT) {
      return TYPE_INT;
    }
    return type;
  }

  private static int compare(PyObject v1, PyObject v2)
  {
    byte t1 = getCompareType(getType(v1));
    byte t2 = getCompareType(getType(v2));
    if (t1 < t2) {
      return -1;
    }
    if (t1 > t2) {
      return 1;
    }
    switch (t1) {
    case TYPE_NONE:
      return 0;

    case TYPE_INT:
      double d1 = v1.asDouble();
      double d2 = v2.asDouble();
      return d1 < d2 ? -1 : d1 > d2 ? 1 : 0;

    case TYPE_STRING:
      // for testing, normalize values to -1/0/1
      int v = v1.asString().compareTo(v2.asString());
      return v > 0 ? 1 : v < 0 ? -1 : 0;

    case TYPE_TUPLE:
      int i = 0;
      for (; i < v1.__len__(); ++i) {
        if (i == v2.__len__()) {
          return 1;
        }
        int c = compare(v1.__getitem__(i), v2.__getitem__(i));
        if (c != 0) {
          return c;
        }
      }
      if (i < v2.__len__()) {
        return -1;
      }
    }
    return 0;
  }

  public int compareTo(Object other) {
    return compare(value, (((TypeWritable) other).value));
  }

  public int hashCode() {
    return value.hashCode();
  }

  public String toString() {
    if (value == null) {
      return "null";
    }
    return value.toString();
  }

  private static class ByteReader
  {
    byte[] bytes_;
    int pos_;

    ByteReader(byte[] bytes, int start)
    {
      bytes_ = bytes;
      pos_ = start;
    }

    byte readByte()
    {
      return bytes_[pos_++];
    }

    int readVInt() throws IOException
    {
      int size = WritableUtils.decodeVIntSize(bytes_[pos_]);
      int r = WritableComparator.readVInt(bytes_, pos_);
      pos_ += size;
      return r;
    }

    long readVLong() throws IOException
    {
      int size = WritableUtils.decodeVIntSize(bytes_[pos_]);
      long r = WritableComparator.readVLong(bytes_, pos_);
      pos_ += size;
      return r;
    }

    double readDouble()
    {
      double r = WritableComparator.readDouble(bytes_, pos_);
      pos_ += 8;
      return r;
    }

    String readString() throws IOException
    {
      int size = readVInt();
      String r = new String(bytes_, pos_, size);
      pos_ += size;
      return r;
    }
  }

  public static class Comparator extends WritableComparator
  {
    public Comparator() {
      super(TypeWritable.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2)
    {
      try {
        return compare(new ByteReader(b1, s1), new ByteReader(b2, s2));
      }
      catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    }

    /**
     * Compares bytes that represent serialized TypeWritable objects.
     * The ByteReaders are at valid ending positions only if this method
     * returns 0.
     */
    static int compare(ByteReader v1, ByteReader v2) throws IOException
    {
      byte t1 = v1.readByte();
      byte t2 = v2.readByte();

      byte c1 = TypeWritable.getCompareType(t1);
      byte c2 = TypeWritable.getCompareType(t2);

      if (c1 < c2) {
        return -1;
      }
      if (c1 > c2) {
        return 1;
      }

      switch (c1) {
      case TYPE_NONE:
        return 0;

      case TYPE_INT:
        double d1;
        if (t1 == TYPE_INT) {
          d1 = v1.readVLong();
        }
        else {
          d1 = v1.readDouble();
        }
        double d2;
        if (t2 == TYPE_INT) {
          d2 = v2.readVLong();
        }
        else {
          d2 = v2.readDouble();
        }
        return d1 < d2 ? -1 : d1 > d2 ? 1 : 0;

      case TYPE_STRING:
        // normalize to -1/0/1 for testing
        int r = v1.readString().compareTo(v2.readString());
        return r > 0 ? 1 : r < 0 ? -1 : 0;

      case TYPE_TUPLE:
        int l1 = v1.readVInt();
        int l2 = v2.readVInt();
        int i = 0;
        for (; i < l1; ++i) {
          if (i == l2) {
            return 1;
          }
          int c = compare(v1, v2);
          if (c != 0) {
            return c;
          }
        }
        if (i < l2) {
          return -1;
        }
      }
      return 0;
    }
  }
}
