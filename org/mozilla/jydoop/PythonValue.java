/* -*- Mode: Java; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

package org.mozilla.jydoop;

import org.python.core.PyObject;
import org.python.core.PyInteger;
import org.python.core.PyLong;
import org.python.core.PyFloat;
import org.python.core.PyString;
import org.python.core.PyTuple;
import org.python.core.Py;
import org.python.core.PyDictionary;
import org.python.core.PyList;
import org.python.core.PySequenceList;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.lang.AssertionError;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import java.util.Map;

/**
 * This class is a Hadoop Writable which is capable of reflecting a defined subset of Python
 * objects as hadoop values.
 *
 * The python types representable by this interface are:
 * - None
 * - int/long
 * - float (java double)
 * - string
 * - tuples of the above types
 * - lists of the above types
 * - dicts of the above types
 *
 * This class is subclassed as PythonKey which can be a hadoop key type. When
 * used as a key, lists and dicts are not permitted.
 */
public class PythonValue implements Writable
{
  // The numeric value of these TYPE_ markers define the sort order
  // of PythonValue instances
  static final byte TYPE_NONE = 0;
  static final byte TYPE_INT = 1;
  static final byte TYPE_FLOAT = 2;
  static final byte TYPE_STRING = 3;
  static final byte TYPE_TUPLE = 4;
  static final byte TYPE_DICT = 5;
  static final byte TYPE_LIST = 6;
  
  public PyObject value;

  protected byte CheckType(PyObject obj)
  {
    if (obj == Py.None) {
      return TYPE_NONE;
    }
    if (obj instanceof PyInteger ||
        obj instanceof PyLong) {
      return TYPE_INT;
    }
    if (obj instanceof PyFloat) {
      return TYPE_FLOAT;
    }
    if (obj instanceof PyString) {
      return TYPE_STRING;
    }

    if (obj instanceof PySequenceList) {
      for (PyObject inner : ((PySequenceList)obj).getArray()) {
        CheckType(inner);
      }
      if (obj instanceof PyTuple) {
        return TYPE_TUPLE;
      }
      if (obj instanceof PyList) {
        return TYPE_LIST;
      }
      // fall through
    }

    if (obj instanceof PyDictionary) {
      for (Map.Entry<PyObject, PyObject> entry : ((PyDictionary)obj).getMap().entrySet()) {
        CheckType(entry.getKey());
        CheckType(entry.getValue());
      }
      return TYPE_DICT;
    }

    throw Py.TypeError("Expected int/float/str/tuple/list/dict, got "+obj.getClass().toString());
  }

  public PythonValue()
  {
  }

  public PythonValue(PyObject obj)
  {
    CheckType(obj);
    value = obj;
  }

  protected static byte getType(PyObject obj)
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
    if (obj instanceof PyList) {
      return TYPE_LIST;
    }
    if (obj instanceof PyDictionary) {
      return TYPE_DICT;
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
    case TYPE_LIST:
      PyObject[] array = ((PySequenceList)obj).getArray();
      WritableUtils.writeVInt(out, array.length);

      for (int i = 0; i < array.length; ++i) {
        WriteType(out, array[i]);
      }
      return;
    case TYPE_DICT:
      PyDictionary dict = (PyDictionary) obj;
      WritableUtils.writeVInt(out, dict.size());
      for (Map.Entry<PyObject, PyObject> entry : dict.getMap().entrySet()) {
        WriteType(out, entry.getKey());
        WriteType(out, entry.getValue());
      }      
      return;
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
    case TYPE_LIST:
      int l = WritableUtils.readVInt(in);
      PyObject[] objs = new PyObject[l];
      for (int i = 0; i < l; ++i) {
        objs[i] = ReadObject(in);
      }
      if (type == TYPE_TUPLE)
        return new PyTuple(objs, false);
      return new PyList(objs);
    case TYPE_DICT:
      int len = WritableUtils.readVInt(in);
      PyDictionary dict = new PyDictionary();
      for (int i = 0; i < len; ++i) {
        PyObject key = ReadObject(in);
        PyObject val = ReadObject(in);
        dict.getMap().put(key, val);
      }
      return dict;
    }
    throw new AssertionError("Unexpected type value");
  }

  public void readFields(DataInput in) throws IOException
  {
    value = ReadObject(in);
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
}
