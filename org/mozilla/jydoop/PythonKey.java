/* -*- Mode: Java; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

package org.mozilla.jydoop;

import java.lang.AssertionError;

import java.io.IOException;

import org.python.core.Py;
import org.python.core.PyObject;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;


public class PythonKey extends PythonValue implements WritableComparable
{
  public PythonKey()
  {
  }

  public PythonKey(PyObject obj)
  {
    CheckType(obj);
    value = obj;
  }

  @Override
  protected byte CheckType(PyObject obj)
  {
    byte t = super.CheckType(obj);
    if (t == TYPE_NONE ||
        t == TYPE_INT ||
        t == TYPE_FLOAT ||
        t == TYPE_STRING ||
        t == TYPE_TUPLE)
      return t;

    throw Py.TypeError("Expected int/float/str/tuple, got "+obj.getClass().toString());
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
      return 0;

    case TYPE_LIST:
    case TYPE_DICT:
      throw new AssertionError("Should not be comparing list/dict");
    }
    return 0;
  }

  public int compareTo(Object other) {
    return compare(value, (((PythonValue) other).value));
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
      super(PythonKey.class);
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
     * Compares bytes that represent serialized PythonValue objects.
     * The ByteReaders are at valid ending positions only if this method
     * returns 0.
     */
    static int compare(ByteReader v1, ByteReader v2) throws IOException
    {
      byte t1 = v1.readByte();
      byte t2 = v2.readByte();

      byte c1 = getCompareType(t1);
      byte c2 = getCompareType(t2);

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
        return 0;

      case TYPE_LIST:
      case TYPE_DICT:
        throw new AssertionError("Should not be comparing list/dict");
      }
      throw new AssertionError("unhandled byte comparison");
    }
  }
}
