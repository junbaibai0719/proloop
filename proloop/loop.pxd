# cython: language_level=3
# distutils: language=c++

# cython: language_level=3
# distutils: language = c++
from libcpp.vector cimport vector
from libcpp.map cimport map
from libcpp.string cimport string
from libcpp.set cimport set
from winbase cimport LPOVERLAPPED, LPVOID, DWORD, ULONG_PTR
from ioapi cimport IOCP
from cpython cimport PyObject


ctypedef PyObject *PyObjectPtr

ctypedef object (*method3_t)(DWORD transferred, ULONG_PTR key, LPOVERLAPPED ov)

ctypedef unsigned long long ulonglong
ctypedef long long longlong

cdef class ProactorEventLoop:
    cdef:
        bint _closed
        IocpProactor __proactor
        object _task_factory
        bint _debug
        object _ready
        object _asyncgens
        bint _stopping
    cdef inline _check_closed(self)
    cdef inline void _run_once(self)

cdef class IocpProactor:
    cdef:
        object _loop
        list _results
        IOCP _iocp
        # map[LPOVERLAPPED, PyObjectPtr] _cache
        dict _cache
        vector[ulonglong] _unregistered
        # object _registered
        # object _stopped_serving
        set[longlong] _registered
        set[longlong] _stopped_serving

    cpdef list select(self, DWORD timeout)
    cpdef void _poll(self, DWORD timeout)