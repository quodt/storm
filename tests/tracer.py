import datetime
import sys

from storm.tracer import (trace, install_tracer, get_tracers,
                          remove_tracer_type, remove_all_tracers, debug,
                          DebugTracer, TimeoutTracer, TimeoutError, _tracers)
from storm.expr import Variable

from tests.helper import TestHelper


class TracerTest(TestHelper):

    def tearDown(self):
        super(TracerTest, self).tearDown()
        del _tracers[:]

    def test_install_tracer(self):
        c = object()
        d = object()
        install_tracer(c)
        install_tracer(d)
        self.assertEquals(get_tracers(), [c, d])

    def test_remove_all_tracers(self):
        install_tracer(object())
        remove_all_tracers()
        self.assertEquals(get_tracers(), [])

    def test_remove_tracer_type(self):
        class C(object): pass
        class D(C): pass
        c = C()
        d1 = D()
        d2 = D()
        install_tracer(d1)
        install_tracer(c)
        install_tracer(d2)
        remove_tracer_type(C)
        self.assertEquals(get_tracers(), [d1, d2])
        remove_tracer_type(D)
        self.assertEquals(get_tracers(), [])

    def test_install_debug(self):
        debug(True)
        debug(True)
        self.assertEquals([type(x) for x in get_tracers()], [DebugTracer])

    def test_wb_install_debug_with_custom_stream(self):
        marker = object()
        debug(True, marker)
        [tracer] = get_tracers()
        self.assertEquals(tracer._stream, marker)

    def test_remove_debug(self):
        debug(True)
        debug(True)
        debug(False)
        self.assertEquals(get_tracers(), [])

    def test_trace(self):
        stash = []
        class Tracer(object):
            def m1(_, *args, **kwargs):
                stash.extend(["m1", args, kwargs])
            def m2(_, *args, **kwargs):
                stash.extend(["m2", args, kwargs])

        install_tracer(Tracer())
        trace("m1", 1, 2, c=3)
        trace("m2")
        trace("m3")
        self.assertEquals(stash, ["m1", (1, 2), {"c": 3}, "m2", (), {}])



class MockVariable(Variable):

    def __init__(self, value):
        self._value = value

    def get(self):
        return self._value


class DebugTracerTest(TestHelper):

    def setUp(self):
        super(DebugTracerTest, self).setUp()
        self.stream = self.mocker.mock(file)
        self.tracer = DebugTracer(self.stream)

        datetime_mock = self.mocker.replace("datetime.datetime")
        datetime_mock.now()
        self.mocker.result(datetime.datetime(1,2,3,4,5,6,7))
        self.mocker.count(0, 1)

        self.variable = MockVariable("PARAM")

    def tearDown(self):
        del _tracers[:]
        super(DebugTracerTest, self).tearDown()

    def test_wb_debug_tracer_uses_stderr_by_default(self):
        self.mocker.replay()

        tracer = DebugTracer()
        self.assertEqual(tracer._stream, sys.stderr)

    def test_wb_debug_tracer_uses_first_arg_as_stream(self):
        self.mocker.replay()

        marker = object()
        tracer = DebugTracer(marker)
        self.assertEqual(tracer._stream, marker)

    def test_connection_raw_execute(self):
        self.stream.write(
            "[04:05:06.000007] EXECUTE: 'STATEMENT', ('PARAM',)\n")
        self.stream.flush()
        self.mocker.replay()

        connection = "CONNECTION"
        raw_cursor = "RAW_CURSOR"
        statement = "STATEMENT"
        params = [self.variable]

        self.tracer.connection_raw_execute(connection, raw_cursor,
                                           statement, params)

    def test_connection_raw_execute_with_non_variable(self):
        self.stream.write(
            "[04:05:06.000007] EXECUTE: 'STATEMENT', ('PARAM', 1)\n")
        self.stream.flush()
        self.mocker.replay()

        connection = "CONNECTION"
        raw_cursor = "RAW_CURSOR"
        statement = "STATEMENT"
        params = [self.variable, 1]

        self.tracer.connection_raw_execute(connection, raw_cursor,
                                           statement, params)

    def test_connection_raw_execute_error(self):
        self.stream.write("[04:05:06.000007] ERROR: ERROR\n")
        self.stream.flush()
        self.mocker.replay()

        connection = "CONNECTION"
        raw_cursor = "RAW_CURSOR"
        statement = "STATEMENT"
        params = "PARAMS"
        error = "ERROR"

        self.tracer.connection_raw_execute_error(connection, raw_cursor,
                                                 statement, params, error)

    def test_connection_raw_execute_success(self):
        self.stream.write("[04:05:06.000007] DONE\n")
        self.stream.flush()
        self.mocker.replay()

        connection = "CONNECTION"
        raw_cursor = "RAW_CURSOR"
        statement = "STATEMENT"
        params = "PARAMS"

        self.tracer.connection_raw_execute_success(connection, raw_cursor,
                                                   statement, params)


class TimeoutTracerTestBase(TestHelper):

    tracer_class = TimeoutTracer

    def setUp(self):
        super(TimeoutTracerTestBase, self).setUp()
        self.tracer = self.tracer_class()
        self.raw_cursor = self.mocker.mock()
        self.statement = self.mocker.mock()
        self.params = self.mocker.mock()

        # Some data is kept in the connection, so we use a proxy to
        # allow things we don't care about here to happen.
        class Connection(object): pass
        self.connection = self.mocker.proxy(Connection())

    def tearDown(self):
        super(TimeoutTracerTestBase, self).tearDown()
        del _tracers[:]

    def execute(self):
        self.tracer.connection_raw_execute(self.connection, self.raw_cursor,
                                           self.statement, self.params)

    def execute_raising(self):
        self.assertRaises(TimeoutError, self.tracer.connection_raw_execute,
                          self.connection, self.raw_cursor,
                          self.statement, self.params)


class TimeoutTracerTest(TimeoutTracerTestBase):

    def test_raise_not_implemented(self):
        """
        L{TimeoutTracer.connection_raw_execute_error},
        L{TimeoutTracer.set_statement_timeout} and
        L{TimeoutTracer.get_remaining_time} must all be implemented by
        backend-specific subclasses.
        """
        self.assertRaises(NotImplementedError,
                          self.tracer.connection_raw_execute_error,
                          None, None, None, None, None)
        self.assertRaises(NotImplementedError,
                          self.tracer.set_statement_timeout, None, None)
        self.assertRaises(NotImplementedError,
                          self.tracer.get_remaining_time)

    def test_raise_timeout_error_when_no_remaining_time(self):
        """
        A L{TimeoutError} is raised if there isn't any time left when a
        statement is executed.
        """
        tracer_mock = self.mocker.patch(self.tracer)
        tracer_mock.get_remaining_time()
        self.mocker.result(0)
        self.mocker.replay()

        try:
            self.execute()
        except TimeoutError, e:
            self.assertEqual("0 seconds remaining in time budget", e.message)
            self.assertEqual(self.statement, e.statement)
            self.assertEqual(self.params, e.params)
        else:
            self.fail("TimeoutError not raised")

    def test_raise_timeout_on_granularity(self):
        tracer_mock = self.mocker.patch(self.tracer)

        self.mocker.order()

        tracer_mock.get_remaining_time()
        self.mocker.result(self.tracer.granularity)
        tracer_mock.set_statement_timeout(self.raw_cursor,
                                          self.tracer.granularity)
        tracer_mock.get_remaining_time()
        self.mocker.result(0)
        self.mocker.replay()

        self.execute()
        self.execute_raising()

    def test_wont_raise_timeout_before_granularity(self):
        tracer_mock = self.mocker.patch(self.tracer)

        self.mocker.order()

        tracer_mock.get_remaining_time()
        self.mocker.result(self.tracer.granularity)
        tracer_mock.set_statement_timeout(self.raw_cursor,
                                          self.tracer.granularity)
        tracer_mock.get_remaining_time()
        self.mocker.result(1)
        self.mocker.replay()

        self.execute()
        self.execute()

    def test_always_set_when_remaining_time_increased(self):
        tracer_mock = self.mocker.patch(self.tracer)

        self.mocker.order()

        tracer_mock.get_remaining_time()
        self.mocker.result(1)
        tracer_mock.set_statement_timeout(self.raw_cursor, 1)
        tracer_mock.get_remaining_time()
        self.mocker.result(2)
        tracer_mock.set_statement_timeout(self.raw_cursor, 2)
        self.mocker.replay()

        self.execute()
        self.execute()

    def test_set_again_on_granularity(self):
        tracer_mock = self.mocker.patch(self.tracer)

        self.mocker.order()

        tracer_mock.get_remaining_time()
        self.mocker.result(self.tracer.granularity * 2)
        tracer_mock.set_statement_timeout(self.raw_cursor,
                                          self.tracer.granularity * 2)
        tracer_mock.get_remaining_time()
        self.mocker.result(self.tracer.granularity)
        tracer_mock.set_statement_timeout(self.raw_cursor,
                                          self.tracer.granularity)
        self.mocker.replay()

        self.execute()
        self.execute()

    def test_set_again_after_granularity(self):
        tracer_mock = self.mocker.patch(self.tracer)

        self.mocker.order()

        tracer_mock.get_remaining_time()
        self.mocker.result(self.tracer.granularity * 2)
        tracer_mock.set_statement_timeout(self.raw_cursor,
                                          self.tracer.granularity * 2)
        tracer_mock.get_remaining_time()
        self.mocker.result(self.tracer.granularity - 1)
        tracer_mock.set_statement_timeout(self.raw_cursor,
                                          self.tracer.granularity - 1)
        self.mocker.replay()

        self.execute()
        self.execute()
