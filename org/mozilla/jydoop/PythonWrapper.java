/* -*- Mode: Java; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

package org.mozilla.jydoop;
import org.python.util.PythonInterpreter;
import org.python.core.PyObject;
import org.python.core.Py;
import org.python.core.PyType;

import java.io.IOException;
import java.io.InputStream;
import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
import java.net.JarURLConnection;

public class PythonWrapper {
  private PythonInterpreter interp;

  public static class JSONImporter extends PyObject {
    public static final String JSON_IMPORT_PATH_ENTRY = "__jsonpath__";

    public PyObject __call__(PyObject args[], String keywords[]) {
      if (args[0].toString().endsWith(JSON_IMPORT_PATH_ENTRY)) {
        return this;
      }
      throw Py.ImportError("unable to handle");
    }

    public PyObject find_module(String name) {
      if (!name.equals("json")) {
	return Py.None;
      }

      return this;
    }

    public PyObject load_module(String name) {
      return PyType.fromClass(JacksonWrapper.class);
    }

    public String toString() {
      return this.getType().toString();
    }
  }

  private File syspathEntry(String path) throws IOException {
    URL url = this.getClass().getResource(path);

    File f;

    try {
      if (url == null) {
        f = new File(path);
      } else if (url.getProtocol().equals("file")) {
        f = new File(url.toURI());
      } else if (url.getProtocol().equals("jar")) {
        JarURLConnection j = (JarURLConnection) url.openConnection();
        File jf = new File(j.getJarFileURL().toURI());
        f = new File(jf, j.getEntryName());
      } else {
        throw new java.lang.UnsupportedOperationException("Cannot get file path for URL: " +
          url);
      }
    } catch (java.net.URISyntaxException e) {
      throw new IOException(e);
    }

    return f;
  }

  PythonWrapper(String pathname) throws IOException {
    interp = new PythonInterpreter();

    // Add the shared modules directory to sys.path.
    File syspathentry = this.syspathEntry("/pylib/");
    interp.getSystemState().path.insert(0, Py.newString(syspathentry.getPath()));

    // Add the location of the script to sys.path so that relative imports work
    syspathentry = this.syspathEntry("/" + pathname);

    interp.getSystemState().path.insert(0, Py.newString(syspathentry.getParent()));

    // Use an import hook so that "import json" uses Jyson instead of the very
    // slow builtin module.
    interp.getSystemState().path.insert(0, Py.newString(JSONImporter.JSON_IMPORT_PATH_ENTRY));
    interp.getSystemState().path_hooks.insert(0, new JSONImporter());

    // Get the the script path from our loader
    URL scripturl = this.getClass().getResource("/" + pathname);
    InputStream pythonstream = scripturl != null ? scripturl.openStream() : new FileInputStream(pathname);
    interp.execfile(pythonstream, pathname);
  }

  public static void main(String args[]) throws IOException {
    new PythonWrapper(args[0]);
  }

  public PyObject getFunction(String name) {
    return interp.get(name.intern());
  }
}
