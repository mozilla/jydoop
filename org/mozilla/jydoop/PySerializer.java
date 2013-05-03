package org.mozilla.jydoop;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import org.python.core.PyDictionary;
import org.python.core.PyObject;
import org.python.core.PyList;
import org.python.core.Py;
import org.python.core.PyUnicode;
import org.python.core.PyInteger;
import org.python.core.PyLong;
import org.python.core.PySequenceList;

import java.lang.AssertionError;

import java.util.ArrayList;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.Map;

 public class PySerializer extends StdSerializer<PyObject> {
     public PySerializer() {
       super(PyObject.class);
    }

    public void serialize(PyObject value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
      if (value instanceof PyDictionary) {
        jgen.writeStartObject();
        for (Map.Entry<PyObject, PyObject> entry : ((PyDictionary)value).getMap().entrySet()) {
          String key = entry.getKey().asString();
          jgen.writeObjectField(key, entry.getValue());
        }
        jgen.writeEndObject();
      } else if(value instanceof PyUnicode) {
        jgen.writeString(value.asString());
      } else if (value instanceof PyInteger || value instanceof PyLong) {
        jgen.writeNumber(value.asLong());
      } else if (value instanceof PySequenceList) {
        PyObject[] array = ((PySequenceList)value).getArray();
        jgen.writeStartArray();
        for (int i = 0; i < array.length; ++i) {
          jgen.writeObject(array[i]);
        }
        jgen.writeEndArray();
      } else {
       throw new Error("Dunno how to serialize:"+value.getClass().getName());
      }
    }
  }

