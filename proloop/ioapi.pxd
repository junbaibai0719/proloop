# cython: language_level=3
# distutils: language = c++
from winbase cimport *

cdef extern from "handleapi.h":
    cdef BOOL CloseHandle(
            HANDLE hObject
    )

cdef extern from "ioapiset.h":
    cdef HANDLE CreateIoCompletionPort(
            HANDLE    FileHandle,
            HANDLE    ExistingCompletionPort,
            ULONG_PTR CompletionKey,
            DWORD     NumberOfConcurrentThreads
    )

    cdef BOOL GetQueuedCompletionStatus(
            HANDLE       CompletionPort,
            LPDWORD      lpNumberOfBytesTransferred,
            PULONG_PTR   lpCompletionKey,
            LPOVERLAPPED *lpOverlapped,
            DWORD        dwMilliseconds
    )

    cdef BOOL GetOverlappedResult(
            HANDLE hFile,
            LPOVERLAPPED lpOverlapped,
            LPDWORD lpNumberOfBytesTransferred,
            BOOL bWait
    )

    cdef BOOL  CancelIo(HANDLE hFile)

ctypedef HANDLE IOCP


# cdef extern from "winsock2.h":
#     ctypedef unsigned int UINT_PTR
#     ctypedef UINT_PTR SOCKET
#     cdef struct sockaddr:
#         pass
#     cdef struct _WSABUF:
#         pass
#     cdef struct _QualityOfService:
#         pass
#     ctypedef _QualityOfService* LPQOS
#     ctypedef _WSABUF* LPWSABUF
#     cdef int WSAConnect(
#       SOCKET         s,
#       const sockaddr *name,
#       int            namelen,
#       LPWSABUF       lpCallerData,
#         LPWSABUF       lpCalleeData,
#       LPQOS          lpSQOS,
#       LPQOS          lpGQOS
#     )