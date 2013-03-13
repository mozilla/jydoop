import org.python.core.PyObject;
import org.python.core.PySystemState;
import org.python.core.Py;

interface MapFunc {
  public void  map(Object... args);
}

// javac -cp jython-2.7b1.jar CallPython.java && java -cp .:jython-2.7b1.jar  CallPython
class CallPython {
  public static void main(String arg[]) {
    //    PyObject o = interpreter.get("test.callme");
    //    Object args[] = {System.out};
    // o._jcall(args);
    PyObject importer = new PySystemState().getBuiltins().__getitem__(Py.newString("__import__"));
    PyObject module = importer.__call__(Py.newString("CallJava"));

    
    PyObject map = module.__getattr__("map");
    try { 
    PyObject reduce = module.__getattr__("reducek");
    System.out.println(""+reduce);
    } catch (Exception e) {
    }

    map._jcall(new Object[] {"hello", "world", "boo"});
 
  }
}
