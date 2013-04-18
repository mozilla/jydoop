"""unit tests for python classes. Run using `make check`"""

import org.python.core

# org.python.core.Options.showJavaExceptions = True

import org.mozilla.jydoop
import unittest
import java.io
import java.lang
import sys

class TestTypeWritable(unittest.TestCase):
    def assertDeepEquals(self, v1, v2):
        """
        Since we should only be running this on primitive types, repr will do a great
        job of deep equality testing.
        """
        r1 = repr(v1)
        r2 = repr(v2)
        self.assertEqual(r1, r2)

    def checkTypeWrapper(self, v1, v2, expected):
        """
        Given two values, do a set of tests on TypeWritable to make sure that comparisons
        and serialization work correctly.
        """

        w1 = org.mozilla.jydoop.TypeWritable(v1)
        w2 = org.mozilla.jydoop.TypeWritable(v2)

        self.assertEqual(w1.compareTo(w2), expected)
        self.assertEqual(w2.compareTo(w1), -expected)

        stream1 = java.io.ByteArrayOutputStream()
        stream2 = java.io.ByteArrayOutputStream()

        do1 = java.io.DataOutputStream(stream1)
        do2 = java.io.DataOutputStream(stream2)

        w1.write(do1)
        w2.write(do2)

        bytes1 = stream1.toByteArray()
        bytes2 = stream2.toByteArray()

        comp = org.mozilla.jydoop.TypeWritable.Comparator()

        self.assertEqual(comp.compare(bytes1, 0, 0, bytes2, 0, 0), expected)
        self.assertEqual(comp.compare(bytes2, 0, 0, bytes1, 0, 0), -expected)

        ww1 = org.mozilla.jydoop.TypeWritable()
        ww2 = org.mozilla.jydoop.TypeWritable()
        ww1.readFields(java.io.DataInputStream(java.io.ByteArrayInputStream(bytes1)))
        ww2.readFields(java.io.DataInputStream(java.io.ByteArrayInputStream(bytes2)))

        self.assertDeepEquals(v1, ww1.value)
        self.assertDeepEquals(v2, ww2.value)

    def test_basic(self):
        self.checkTypeWrapper(None, None, 0)
        self.checkTypeWrapper(None, 0, -1)
        self.checkTypeWrapper(None, 0.0, -1)
        self.checkTypeWrapper(None, "foo", -1)
        self.checkTypeWrapper(None, (), -1)
        self.checkTypeWrapper(17, 17, 0)
        self.checkTypeWrapper(17, 14, 1)
        self.checkTypeWrapper(17, 17.0, 0)
        self.checkTypeWrapper(21, 13.0, 1)
        self.checkTypeWrapper(17, "foo", -1)
        self.checkTypeWrapper(17, (), -1)
        self.checkTypeWrapper(128.0, 128.0, 0)
        self.checkTypeWrapper(17.0, "foo", -1)
        self.checkTypeWrapper(17.0, (), -1)
        self.checkTypeWrapper("foo", "foo", 0)
        self.checkTypeWrapper("foo", "bar", 1)
        self.checkTypeWrapper("", "foobar", -1)
        self.checkTypeWrapper((), (), 0)
        self.checkTypeWrapper((1,), (), 1)
        self.checkTypeWrapper(("foo",), ("foo",), 0)

class TestResource(unittest.TestCase):
    def test_basic(self):
        import sys
        sys.path.append('scripts')
        import jydoop
        self.assertIsNotNone(jydoop.getResource("scripts/jydoop.py"))

if __name__ == '__main__':
    unittest.main()
