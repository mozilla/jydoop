/* -*- Mode: Java; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
package org.mozilla.pydoop;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import org.python.core.PyObject;
import org.python.core.PyDictionary;
import org.python.core.PyList;
import org.python.core.Py;

import java.lang.AssertionError;

import java.util.ArrayList;

import java.io.IOException;

public class JacksonWrapper
{
  static JsonFactory factory;

  private static void addValue(PyObject v, PyObject container,
                               JsonToken containerType, JsonParser parser)
    throws IOException
  {
    switch (containerType) {
    case START_OBJECT:
      container.__setitem__(Py.newUnicode(parser.getCurrentName()), v);
      break;
    case START_ARRAY:
      ((PyList) container).append(v);
      break;
    default:
      throw new AssertionError("Unexpected container type");
    }
  }

  private static PyObject convertNumber(JsonParser parser) throws IOException
  {
    JsonParser.NumberType nt = parser.getNumberType();
    switch (nt) {
    case BIG_DECIMAL:
      throw Py.ValueError("decimal outside of double range");

    case BIG_INTEGER:
      return Py.newLong(parser.getBigIntegerValue());

    case DOUBLE:
    case FLOAT:
      return Py.newFloat(parser.getDoubleValue());

    case INT:
    case LONG:
      return Py.newInteger(parser.getLongValue());

    default:
      throw new AssertionError("Unexpected JsonParser.NumberType");
    }
  }

  /**
   * Take an event stream from a JsonParser and turn it into a PyObject.
   *
   * TODO: enhance this API by indicating that you're only interested in certain key paths.
   * TODO: enhance this API by allowing the caller to supply different classes instead of list/dict.
   */
  public static PyObject loads(String s) throws IOException
  {
    if (factory == null) {
      factory = new JsonFactory();
    }
    JsonParser parser = factory.createJsonParser(s);

    ArrayList<PyObject> containers = new ArrayList<PyObject>();
    ArrayList<JsonToken> containerTypes = new ArrayList<JsonToken>();

    PyObject container = new PyList();
    JsonToken type = JsonToken.START_ARRAY;

    JsonToken current;
    while ((current = parser.nextToken()) != null) {
      PyObject v;
      switch (current) {
      case END_ARRAY:
      case END_OBJECT:
        // Oh I wish for .pop()
        container = containers.remove(containers.size() - 1);
        type = containerTypes.remove(containerTypes.size() - 1);
        break;
      case FIELD_NAME:
        break;
      case START_ARRAY:
        PyList l = new PyList();
        addValue(l, container, type, parser);
        containers.add(container);
        containerTypes.add(type);
        container = l;
        type = current;
        break;
      case START_OBJECT:
        PyDictionary d = new PyDictionary();
        addValue(d, container, type, parser);
        containers.add(container);
        containerTypes.add(type);
        container = d;
        type = current;
        break;
      case VALUE_FALSE:
        addValue(Py.False, container, type, parser);
        break;
      case VALUE_TRUE:
        addValue(Py.True, container, type, parser);
        break;

      case VALUE_NUMBER_FLOAT:
      case VALUE_NUMBER_INT:
        addValue(convertNumber(parser), container, type, parser);
        break;

      case VALUE_STRING:
        addValue(Py.newUnicode(parser.getText()), container, type, parser);
        break;

      case VALUE_NULL:
        addValue(Py.None, container, type, parser);
        break;

      default:
        throw new AssertionError("Unexpected JSON token type");
      }
    }

    assert containers.size() == 0;
    assert containerTypes.size() == 0;
    assert type == JsonToken.START_ARRAY;
    PyList containerList = (PyList) container;
    if (containerList.size() != 1) {
      throw Py.TypeError("No or multiple json root objects");
    }
    return containerList.__getitem__(0);
  }
}
