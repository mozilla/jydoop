package taras;
import org.python.core.PyObject;
import org.python.core.PySystemState;
import org.python.core.Py;

public class PythonWrapper {
  private static PyObject pythonModule;
  public static PyObject get(String attr) {
    if (pythonModule == null) {
      PyObject importer = new PySystemState().getBuiltins().__getitem__(Py.newString("__import__"));
      pythonModule = importer.__call__(Py.newString("CallJava"));
    }
    try {
      return pythonModule.__getattr__(attr);
    } catch(Exception e) {
      return null;
    }
  }
}
