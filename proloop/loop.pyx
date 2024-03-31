# cython: language_level=3
# distutils: language = c++

from winbase cimport INVALID_HANDLE_VALUE, LPOVERLAPPED, PULONG_PTR, DWORD, ULONG_PTR, GlobalAlloc, GPTR, OVERLAPPED, GetLastError, HANDLE
from ioapi cimport CreateIoCompletionPort, GetQueuedCompletionStatus, CloseHandle, CancelIo
from libc cimport math
from libc.stdio cimport printf
from libcpp cimport bool

import weakref
import asyncio
from asyncio import events
from asyncio import futures
from asyncio import tasks
import socket
import _overlapped
import  time
import io
import sys
import warnings
import collections

cdef longlong cost_sum = 0
cdef longlong select_cost_sum = 0
cdef double run_once_cost_sum = 0
cdef double run_once_other_cost_sum = 0
cdef double call_soon_cost_sum = 0


cdef DWORD INFINITE = 0xffffffff

# class _OverlappedFuture(futures.Future):
    # ...
from asyncio.windows_events import _OverlappedFuture, _WaitCancelFuture

cdef class _Cache:
    cdef:
        object f
        LPOVERLAPPED ov
        object obj
        void* callback

cdef isfuture(obj):
    if asyncio.isfuture(obj) is None:
        return isinstance(obj, futures.Future)
    else:
        return asyncio.isfuture(obj)


cdef class ProactorEventLoop:
    def __cinit__(self, IocpProactor proactor):
        self.__proactor = proactor
        self._task_factory = None
        self._debug = False
        self._closed = 0
        self._ready = collections.deque()
        self._asyncgens = weakref.WeakSet()
        self._stopping = False

    @property
    def _proactor(self):
        return self.__proactor


    cdef inline _check_closed(self):
        if self._closed == 1:
            raise RuntimeError('Event loop is closed')

    def get_debug(self):
        return self._debug

    cdef inline void _run_once(self):
        # print("run_once")
        timeout = -1
        if self._ready or self._stopping:
            timeout = 0
        # print("time out", timeout, self._ready)
        global select_cost_sum
        start = time.time_ns()
        self.__proactor.select(timeout)
        end = time.time_ns()
        select_cost_sum += end - start
        # print("event_list", event_list)
        ntodo = len(self._ready)
        for i in range(ntodo):
            handle = self._ready.popleft()
            if handle._cancelled:
                continue
            if self._debug:
                try:
                    self._current_handle = handle
                    t0 = self.time()
                    handle._run()
                    dt = self.time() - t0
                finally:
                    self._current_handle = None
            else:
                start = time.time_ns()
                # print(handle)
                handle._run()
                end = time.time_ns()
        handle = None  # Needed to break cycles when an exception occurs.
        global run_once_other_cost_sum
        run_once_other_cost_sum += end - start

    def call_soon(self, callback, *args, context=None):
        """Arrange for a callback to be called as soon as possible.

        This operates as a FIFO queue: callbacks are called in the
        order in which they are registered.  Each callback will be
        called exactly once.

        Any positional arguments after the callback will be passed to
        the callback when it is called.
        """
        # if len(args) > 0 and isinstance(args[0], _OverlappedFuture):
            # raise
        # print("call soon", callback, *args, context)
        start = time.time_ns()
        self._check_closed()
        if self._debug:
            self._check_thread()
            self._check_callback(callback, 'call_soon')
        handle = self._call_soon(callback, args, context)
        if handle._source_traceback:
            del handle._source_traceback[-1]
        end = time.time_ns()
        global call_soon_cost_sum
        call_soon_cost_sum += end - start
        return handle

    def _call_soon(self, callback, args, context):
        handle = events.Handle(callback, args, self, context)
        if handle._source_traceback:
            del handle._source_traceback[-1]
        self._ready.append(handle)
        return handle

    def create_task(self, coro, *, name=None):
        """Schedule a coroutine object.

        Return a task object.
        """
        self._check_closed()
        if self._task_factory is None:
            task = tasks.Task(coro, loop=self, name=name)
            if task._source_traceback:
                del task._source_traceback[-1]
        else:
            task = self._task_factory(self, coro)
            tasks._set_task_name(task, name)

        return task

    def _asyncgen_firstiter_hook(self, agen):
        if self._asyncgens_shutdown_called:
            warnings.warn(
                f"asynchronous generator {agen!r} was scheduled after "
                f"loop.shutdown_asyncgens() call",
                ResourceWarning, source=self)

        self._asyncgens.add(agen)

    def _asyncgen_finalizer_hook(self, agen):
        self._asyncgens.discard(agen)
        if not self.is_closed():
            self.call_soon_threadsafe(self.create_task, agen.aclose())

    def call_soon_threadsafe(self, callback, *args, context=None):
        """Like call_soon(), but thread-safe."""
        self._check_closed()
        if self._debug:
            self._check_callback(callback, 'call_soon_threadsafe')
        handle = self._call_soon(callback, args, context)
        if handle._source_traceback:
            del handle._source_traceback[-1]
        self._write_to_self()
        return handle

    
    def _write_to_self(self):
        # This may be called from a different thread, possibly after
        # _close_self_pipe() has been called or even while it is
        # running.  Guard for self._csock being None or closed.  When
        # a socket is closed, send() raises OSError (with errno set to
        # EBADF, but let's not rely on the exact error code).
        print("_write_to_self")
        return

    def run_forever(self):
        self._check_closed()
        old_agen_hooks = sys.get_asyncgen_hooks()
        sys.set_asyncgen_hooks(firstiter=self._asyncgen_firstiter_hook,
                               finalizer=self._asyncgen_finalizer_hook)
        try:
            events._set_running_loop(self)
            while True:
                start = time.time_ns()
                self._run_once()
                end = time.time_ns()
                global run_once_cost_sum
                run_once_cost_sum += end - start
                if self._stopping:
                    print("stoping", self._stopping)
                    break
        finally:
            events._set_running_loop(None)
            sys.set_asyncgen_hooks(*old_agen_hooks)

    def run_until_complete(self, future):
        self._check_closed()

        new_task = not isfuture(future)
        future:futures.Future = asyncio.ensure_future(future, loop=self)
        if new_task:
            future._log_destroy_pending = False

        def done_cb(fut):
            # print("done_cb", fut)
            # print(fut.exception())
            if not fut.cancelled():
                exc = fut.exception()
                if isinstance(exc, (SystemExit, KeyboardInterrupt)):
                    # Issue #336: run_forever() already finished,
                    # no need to stop it.
                    return
            self.stop()
            # print("self._stoping", self._stopping)

        future.add_done_callback(done_cb)
        try:
            self.run_forever()
        except BaseException:
            if new_task and future.done() and not future.cancelled():
                # The coroutine raised a BaseException. Consume the exception
                # to not log a warning, the caller doesn't have access to the
                # local task.
                future.exception()
            raise
        finally:
            future.remove_done_callback(done_cb)
        if not future.done():
            raise RuntimeError('Event loop stopped before Future completed.')

        return future.result()

    def call_exception_handler(self, context):
        """Call the current event loop's exception handler.

        The context argument is a dict containing the following keys:

        - 'message': Error message;
        - 'exception' (optional): Exception object;
        - 'future' (optional): Future instance;
        - 'task' (optional): Task instance;
        - 'handle' (optional): Handle instance;
        - 'protocol' (optional): Protocol instance;
        - 'transport' (optional): Transport instance;
        - 'socket' (optional): Socket instance;
        - 'asyncgen' (optional): Asynchronous generator that caused
                                 the exception.

        New keys maybe introduced in the future.

        Note: do not overload this method in an event loop subclass.
        For custom exception handling, use the
        `set_exception_handler()` method.
        """
        print(context)

    def stop(self):
        self._stopping = True

    def close(self):
        self._proactor.close()
        printf("select_cost_sum %I64d ns\n", select_cost_sum)
        printf("run_once_cost_sum: %fms\n", run_once_cost_sum/1000/1000)
        printf("run_once_other_cost_sum: %fms\n", run_once_other_cost_sum/1000/1000)
        printf("call_soon_cost_sum: %fms\n", call_soon_cost_sum/1000/1000)


    def __del__(self):
        self.close()

cdef class IocpProactor:

    def __cinit__(self, int concurrency=0xffffffff):
        self._loop = None
        self._results = []
        self._iocp = CreateIoCompletionPort(<void*>INVALID_HANDLE_VALUE, NULL, 0, concurrency)
        # self._cache = map[LPOVERLAPPED, PyObjectPtr]()
        self._cache = {}
        self._unregistered = vector[ulonglong]()
        # self._registered = weakref.WeakSet()
        # self._stopped_serving = weakref.WeakSet()
        self._registered = set[longlong]()
        self._stopped_serving = set[longlong]()


    def _check_closed(self):
        if not self._iocp:
            raise RuntimeError('IocpProactor is closed')

    def __repr__(self):
        info = ['overlapped#=%s' % len(self._cache),
                'result#=%s' % len(self._results)]
        if not self._iocp:
            info.append('closed')
        return '<%s %s>' % (self.__class__.__name__, " ".join(info))

    def set_loop(self, loop):
        self._loop = loop

    cpdef list select(self, DWORD timeout):
        if timeout is None:
            timeout = -1
        if not self._results:
            self._poll(timeout)
        tmp = self._results
        self._results = []
        return tmp

    def _result(self, value):
        fut = self._loop.create_future()
        fut.set_result(value)
        return fut

    def _register_with_iocp(self, object obj):
        # To get notifications of finished ops on this objects sent to the
        # completion port, were must register the handle.
        cdef longlong fd = obj.fileno()
        cdef HANDLE handle = <HANDLE>(fd)
        if not self._registered.count(fd):
            self._registered.insert(fd)
            CreateIoCompletionPort(handle, self._iocp, 0, 0)

    def _register(self, ov, obj, callback):
        self._check_closed()

        # Return a future which will be set with the result of the
        # operation when it completes.  The future's value is actually
        # the value returned by callback().
        f = _OverlappedFuture(ov, loop=self._loop)
        if f._source_traceback:
            del f._source_traceback[-1]
        
        if not ov.pending:
            # The operation has completed, so no need to postpone the
            # work.  We cannot take this short cut if we need the
            # NumberOfBytes, CompletionKey values returned by
            # PostQueuedCompletionStatus().
            try:
                value = callback(None, None, ov)
            except OSError as e:
                f.set_exception(e)
            else:
                f.set_result(value)
            # Even if GetOverlappedResult() was called, we have to wait for the
            # notification of the completion in GetQueuedCompletionStatus().
            # Register the overlapped operation to keep a reference to the
            # OVERLAPPED object, otherwise the memory is freed and Windows may
            # read uninitialized memory.

        # Register the overlapped operation for later.  Note that
        # we only store obj to prevent it from being garbage
        # collected too early.
        # cdef _Cache _cache = _Cache()
        # _cache.f = f
        # _cache.ov = <LPOVERLAPPED>ov.address
        # _cache.obj = obj
        # _cache.callback = <void*>callback
        # self._cache[<LPOVERLAPPED>ov.address] = <PyObjectPtr>_cache.id()
        self._cache[ov.address] = (f, ov, obj, callback)
        return f

    def recv(self, conn, nbytes, flags=0):
        self._register_with_iocp(conn)
        ov = _overlapped.Overlapped(0)
        try:
            if isinstance(conn, socket.socket):
                ov.WSARecv(conn.fileno(), nbytes, flags)
            else:
                ov.ReadFile(conn.fileno(), nbytes)
        except BrokenPipeError:
            return self._result(b'')

        def finish_recv(trans, key, ov):
            try:
                return ov.getresult()
            except OSError as exc:
                if exc.winerror in (_overlapped.ERROR_NETNAME_DELETED,
                                    _overlapped.ERROR_OPERATION_ABORTED):
                    raise ConnectionResetError(*exc.args)
                else:
                    raise

        return self._register(ov, conn, finish_recv)

    def _unregister(self, ov):
        """Unregister an overlapped object.

        Call this method when its future has been cancelled. The event can
        already be signalled (pending in the proactor event queue). It is also
        safe if the event is never signalled (because it was cancelled).
        """
        self._check_closed()
        self._unregistered.push_back(ov.address)
        # self._cache.erase(<LPOVERLAPPED>ov.address)

    cpdef void _poll(self, DWORD timeout):
        cdef DWORD ms
        if timeout == -1:
            ms = INFINITE
        else:
            ms = <DWORD>math.ceil(timeout * 1e3)
            if ms >= INFINITE:
                raise ValueError("timeout too big")
        cdef DWORD transferred = 0
        cdef ULONG_PTR key = 0
        cdef LPOVERLAPPED overlapped = NULL
        cdef bool status = False
        cdef _Cache _cache
        cdef longlong obj_fd
        cdef longlong start, end
        while True:
            start = time.time_ns()
            status = GetQueuedCompletionStatus(
                self._iocp, &transferred, &key, &overlapped, ms
            )
            if status == 0:
                break
            ms = 0
            # if self._cache.count(overlapped):
            #     _cache = <_Cache>self._cache.at(overlapped)
            #     self._cache.erase(overlapped)
            # else:
            #     if key != 0 or key != INVALID_HANDLE_VALUE:
            #         CloseHandle(<HANDLE>key)
            #     continue
            try:
                f, ov, obj, callback = self._cache.pop(<ulonglong>overlapped)
            except KeyError:
                # key is either zero, or it is used to return a pipe
                # handle which should be closed to avoid a leak.
                if key !=0 or key != INVALID_HANDLE_VALUE:
                    CloseHandle(<HANDLE>key)
                continue
            # f = _cache.f
            if isinstance(obj, int):
                # obj.fileno() is used to get the file descriptor
                obj_fd = <longlong>obj
            else:
                obj_fd = obj.fileno()
            if self._stopped_serving.count(obj_fd):
                f.cancel()
            elif not f.done():
                try:
                    value = callback(transferred, key, ov)
                except OSError as e:
                    f.set_exception(e)
                    self._results.append(f)
                else:
                    f.set_result(value)
                    self._results.append(f)
            end = time.time_ns()
            # if end - start > 0:
            global cost_sum
            cost_sum += end - start
            # print("run once cost", end-start)
        for ov1 in self._unregistered:
            self._cache.pop(<unsigned long long>ov1)
        self._unregistered.clear()

    def close(self):
        if not self._iocp:
            # already closed
            return

        # Cancel remaining registered operations.
        for address, (fut, ov, obj, callback) in list(self._cache.items()):
            if fut.cancelled():
                # Nothing to do with cancelled futures
                pass
            elif isinstance(fut, _WaitCancelFuture):
                # _WaitCancelFuture must not be cancelled
                pass
            else:
                try:
                    fut.cancel()
                except OSError as exc:
                    if self._loop is not None:
                        context = {
                            'message': 'Cancelling a future failed',
                            'exception': exc,
                            'future': fut,
                        }
                        if fut._source_traceback:
                            context['source_traceback'] = fut._source_traceback
                        self._loop.call_exception_handler(context)

        # Wait until all cancelled overlapped complete: don't exit with running
        # overlapped to prevent a crash. Display progress every second if the
        # loop is still running.
        cdef int msg_update = 1
        start_time = time.monotonic()
        next_msg = start_time + msg_update
        while self._cache:
            # handle a few events, or timeout
            self._poll(msg_update)

        self._results = []

        CloseHandle(self._iocp)
        self._iocp = NULL
        global cost_sum
        cdef double unit = 1000
        cdef double cs = <double> cost_sum
        cs = cs/unit
        cs = cs/unit
        printf("poll cost %f ms %I64dns\n", cs, cost_sum)

    def __del__(self):
        self.close()