import org.python.core.PyObject;
import org.python.core.PyString;
import org.python.util.PythonInterpreter;
// javac -cp jython-2.7b1.jar CallPython.java && java -cp .:jython-2.7b1.jar  CallPython
class CallPython {
  public static void main(String arg[]) {
    System.out.println("from java");
    PythonInterpreter interpreter = new PythonInterpreter();
    interpreter.exec("import CallJava");
    interpreter.set("a", System.out);
    interpreter.exec("CallJava.callme(a)");
    //    PyObject o = interpreter.get("test.callme");
    //    Object args[] = {System.out};
    // o._jcall(args);
  }
}
