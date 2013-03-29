package org.mozilla.pydoop;

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

  private static void WriteType(DataOutput out, PyObject obj) throws IOException
  {
    if (obj == Py.None) {
      out.writeByte(TYPE_NONE);
      return;
    }

    if (obj instanceof PyInteger ||
	obj instanceof PyLong) {
      out.writeByte(TYPE_INT);
      WritableUtils.writeVLong(out, obj.asLong());
      return;
    }

    if (obj instanceof PyFloat) {
      out.writeByte(TYPE_FLOAT);
      out.writeDouble(obj.asDouble());
      return;
    }

    if (obj instanceof PyString) {
      out.writeByte(TYPE_STRING);
      out.writeUTF(obj.asString());
      return;
    }

    if (obj instanceof PyTuple) {
      out.writeByte(TYPE_TUPLE);
      WritableUtils.writeVInt(out, obj.__len__());
      for (int i = 0; i < obj.__len__(); ++i) {
	WriteType(out, obj.__getitem__(i));
      }
    }

    throw new AssertionError("Unexpected type");
  }

  public void write(DataOutput out) throws IOException
  {
    WriteType(out, value);
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
      return new PyString(in.readUTF());
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

  public int compareTo(Object other) {
    return value.__cmp__(((TypeWritable) other).value);
  }

  public int hashCode() {
    return value.hashCode();
  }
}
