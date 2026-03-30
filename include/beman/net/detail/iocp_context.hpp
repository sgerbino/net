// include/beman/net/detail/iocp_context.hpp                    -*-C++-*-
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#ifndef INCLUDED_BEMAN_NET_DETAIL_IOCP_CONTEXT
#define INCLUDED_BEMAN_NET_DETAIL_IOCP_CONTEXT

// ----------------------------------------------------------------------------

#include <beman/net/detail/container.hpp>
#include <beman/net/detail/context_base.hpp>
#include <beman/net/detail/sorted_list.hpp>

#include <MSWSock.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <iostream>
#include <system_error>

// ----------------------------------------------------------------------------

namespace beman::net::detail {

struct iocp_record {
    native_handle_type handle;
    int                address_family{AF_INET6};
    bool               blocking{true};
};

// Base OVERLAPPED wrapper for all IOCP operations.
// The OVERLAPPED field must be the first member so that a pointer to this
// struct can be cast to/from LPOVERLAPPED.
struct iocp_overlapped {
    OVERLAPPED overlapped{};
    io_base*   completion{};
    int        result{};
};

// Extended overlapped data for AcceptEx operations.
struct iocp_accept_data : iocp_overlapped {
    SOCKET accept_socket{INVALID_SOCKET};
    char   buffer[2 * (sizeof(::sockaddr_storage) + 16)]{};
};

// Extended overlapped data for WSARecv/WSASend operations.
struct iocp_io_data : iocp_overlapped {
    WSABUF wsabuf{};
    DWORD  flags{};
};

// ----------------------------------------------------------------------------
// io_context implementation based on Windows I/O Completion Ports.

struct iocp_context final : context_base {
    HANDLE                  iocp_handle;
    container<iocp_record>  sockets;
    task*                   tasks{};
    ::std::size_t           outstanding{0};

    // Timer support (same pattern as poll_context)
    using time_t       = ::std::chrono::system_clock::time_point;
    using timer_node_t = context_base::resume_at_operation;
    struct get_time {
        auto operator()(auto* t) const -> time_t { return ::std::get<0>(*t); }
    };
    using timer_priority_t = sorted_list<timer_node_t, ::std::less<>, get_time>;
    timer_priority_t timeouts;

    // Extension function pointers (loaded at runtime via WSAIoctl)
    LPFN_ACCEPTEX              accept_ex_fn{};
    LPFN_GETACCEPTEXSOCKADDRS  get_accept_ex_sockaddrs_fn{};
    LPFN_CONNECTEX             connect_ex_fn{};

    // -----------------------------------------------------------------------
    // Construction / destruction

    iocp_context() {
        WSADATA wsa_data;
        if (int rc = ::WSAStartup(MAKEWORD(2, 2), &wsa_data); rc != 0) {
            throw ::std::system_error(rc, ::std::system_category(), "WSAStartup failed");
        }

        iocp_handle = ::CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 0);
        if (iocp_handle == NULL) {
            ::WSACleanup();
            throw ::std::system_error(
                static_cast<int>(::GetLastError()),
                ::std::system_category(),
                "CreateIoCompletionPort failed");
        }

        load_extension_functions();
    }

    ~iocp_context() override {
        ::CloseHandle(iocp_handle);
        ::WSACleanup();
    }

    // -----------------------------------------------------------------------
    // Helpers

    auto load_extension_functions() -> void {
        SOCKET tmp = ::WSASocketW(AF_INET, SOCK_STREAM, IPPROTO_TCP, NULL, 0, WSA_FLAG_OVERLAPPED);
        if (tmp == INVALID_SOCKET) {
            throw ::std::system_error(
                ::WSAGetLastError(), ::std::system_category(),
                "WSASocket failed loading extension functions");
        }

        DWORD bytes{};
        GUID  guid;

        guid = WSAID_ACCEPTEX;
        ::WSAIoctl(tmp, SIO_GET_EXTENSION_FUNCTION_POINTER,
                   &guid, sizeof(guid),
                   &accept_ex_fn, sizeof(accept_ex_fn),
                   &bytes, NULL, NULL);

        guid = WSAID_GETACCEPTEXSOCKADDRS;
        ::WSAIoctl(tmp, SIO_GET_EXTENSION_FUNCTION_POINTER,
                   &guid, sizeof(guid),
                   &get_accept_ex_sockaddrs_fn, sizeof(get_accept_ex_sockaddrs_fn),
                   &bytes, NULL, NULL);

        guid = WSAID_CONNECTEX;
        ::WSAIoctl(tmp, SIO_GET_EXTENSION_FUNCTION_POINTER,
                   &guid, sizeof(guid),
                   &connect_ex_fn, sizeof(connect_ex_fn),
                   &bytes, NULL, NULL);

        ::closesocket(tmp);
    }

    auto associate_with_iocp(SOCKET s) -> void {
        if (::CreateIoCompletionPort(reinterpret_cast<HANDLE>(s), iocp_handle, 0, 0) == NULL) {
            throw ::std::system_error(
                static_cast<int>(::GetLastError()),
                ::std::system_category(),
                "CreateIoCompletionPort association failed");
        }
    }

    auto process_task() -> ::std::size_t {
        if (tasks) {
            auto* t = tasks;
            tasks   = t->next;
            t->complete();
            return 1u;
        }
        return 0u;
    }

    auto process_timeout(const auto& now) -> ::std::size_t {
        if (!timeouts.empty() && ::std::get<0>(*timeouts.front()) <= now) {
            timeouts.pop_front()->complete();
            return 1u;
        }
        return 0u;
    }

    // -----------------------------------------------------------------------
    // context_base: socket management

    auto make_socket(native_handle_type handle) -> socket_id override {
        return sockets.insert(iocp_record{handle});
    }

    auto make_socket(int d, int t, int p, ::std::error_code& error) -> socket_id override {
        SOCKET s = ::WSASocketW(d, t, p, NULL, 0, WSA_FLAG_OVERLAPPED);
        if (s == INVALID_SOCKET) {
            error = ::std::error_code(::WSAGetLastError(), ::std::system_category());
            return socket_id::invalid;
        }
        associate_with_iocp(s);
        return sockets.insert(iocp_record{static_cast<native_handle_type>(s), d});
    }

    auto release(socket_id id, ::std::error_code& error) -> void override {
        native_handle_type handle = sockets[id].handle;
        sockets.erase(id);
        if (::closesocket(static_cast<SOCKET>(handle)) == SOCKET_ERROR) {
            error = ::std::error_code(::WSAGetLastError(), ::std::system_category());
        }
    }

    auto native_handle(socket_id id) -> native_handle_type override {
        return sockets[id].handle;
    }

    auto set_option(socket_id          id,
                    int                level,
                    int                name,
                    const void*        data,
                    ::socklen_t        size,
                    ::std::error_code& error) -> void override {
        if (::setsockopt(static_cast<SOCKET>(native_handle(id)),
                         level, name,
                         static_cast<const char*>(data), size) == SOCKET_ERROR) {
            error = ::std::error_code(::WSAGetLastError(), ::std::system_category());
        }
    }

    auto bind(socket_id                              id,
              const ::beman::net::detail::endpoint&   ep,
              ::std::error_code&                      error) -> void override {
        if (::bind(static_cast<SOCKET>(native_handle(id)),
                   ep.data(), ep.size()) == SOCKET_ERROR) {
            error = ::std::error_code(::WSAGetLastError(), ::std::system_category());
        }
    }

    auto listen(socket_id id, int no, ::std::error_code& error) -> void override {
        if (::listen(static_cast<SOCKET>(native_handle(id)), no) == SOCKET_ERROR) {
            error = ::std::error_code(::WSAGetLastError(), ::std::system_category());
        }
    }

    // -----------------------------------------------------------------------
    // context_base: event loop

    auto run_one() -> ::std::size_t override {
        auto now = ::std::chrono::system_clock::now();
        if (process_timeout(now) > 0 || process_task() > 0) {
            return 1u;
        }

        if (outstanding == 0 && timeouts.empty()) {
            return 0u;
        }

        // Compute IOCP wait timeout from the nearest timer
        DWORD timeout_ms = INFINITE;
        if (!timeouts.empty()) {
            auto next = ::std::get<0>(*timeouts.front());
            if (next <= now) {
                timeout_ms = 0;
            } else {
                auto ms = ::std::chrono::duration_cast<::std::chrono::milliseconds>(next - now).count();
                timeout_ms = static_cast<DWORD>(::std::min<long long>(ms, INFINITE - 1));
            }
        }

        DWORD        bytes_transferred{};
        ULONG_PTR    completion_key{};
        LPOVERLAPPED overlapped_ptr{};

        BOOL ok = ::GetQueuedCompletionStatus(
            iocp_handle,
            &bytes_transferred,
            &completion_key,
            &overlapped_ptr,
            timeout_ms);

        if (overlapped_ptr == NULL) {
            if (!ok) {
                if (::GetLastError() == WAIT_TIMEOUT) {
                    return process_timeout(::std::chrono::system_clock::now());
                }
                return 0u;
            }
            // Wakeup from PostQueuedCompletionStatus (schedule)
            return process_task();
        }

        // I/O completion
        --outstanding;
        auto* iocp_ol   = reinterpret_cast<iocp_overlapped*>(overlapped_ptr);
        iocp_ol->result = ok
            ? static_cast<int>(bytes_transferred)
            : -static_cast<int>(::GetLastError());

        auto* comp = iocp_ol->completion;
        comp->work(*this, comp);

        return 1u;
    }

    // -----------------------------------------------------------------------
    // context_base: cancellation and scheduling

    auto cancel(io_base* cancel_op, io_base* op) -> void override {
        // Try to cancel a timer first
        if (timeouts.erase(op)) {
            op->cancel();
            cancel_op->cancel();
            return;
        }

        // Cancel pending I/O via CancelIoEx.
        // The cancelled operation will complete through IOCP with
        // ERROR_OPERATION_ABORTED, which the work function handles.
        if (op->id != socket_id::invalid) {
            auto* data = static_cast<iocp_overlapped*>(op->extra.get());
            if (data) {
                ::CancelIoEx(
                    reinterpret_cast<HANDLE>(native_handle(op->id)),
                    &data->overlapped);
            }
        }
        cancel_op->cancel();
    }

    auto schedule(task* t) -> void override {
        t->next = tasks;
        tasks   = t;
        ::PostQueuedCompletionStatus(iocp_handle, 0, 0, NULL);
    }

    // -----------------------------------------------------------------------
    // context_base: async operations

    auto accept(accept_operation* op) -> submit_result override {
        SOCKET listen_socket = static_cast<SOCKET>(native_handle(op->id));
        int    family        = sockets[op->id].address_family;

        auto* data           = new iocp_accept_data{};
        data->completion     = op;
        data->accept_socket  = ::WSASocketW(
            family, SOCK_STREAM, IPPROTO_TCP, NULL, 0, WSA_FLAG_OVERLAPPED);

        if (data->accept_socket == INVALID_SOCKET) {
            int err = ::WSAGetLastError();
            delete data;
            op->error(::std::error_code(err, ::std::system_category()));
            return submit_result::error;
        }

        op->extra = io_base::extra_t(data, [](void* p) {
            auto* d = static_cast<iocp_accept_data*>(p);
            if (d->accept_socket != INVALID_SOCKET) {
                ::closesocket(d->accept_socket);
            }
            delete d;
        });

        op->work = [](context_base& ctx, io_base* io) -> submit_result {
            auto* data     = static_cast<iocp_accept_data*>(io->extra.get());
            auto& iocp_ctx = static_cast<iocp_context&>(ctx);

            if (data->result < 0) {
                int err = -data->result;
                if (err == static_cast<int>(ERROR_OPERATION_ABORTED)) {
                    io->cancel();
                    return submit_result::ready;
                }
                io->error(::std::error_code(err, ::std::system_category()));
                return submit_result::error;
            }

            auto* aop = static_cast<accept_operation*>(io);

            // Parse addresses from AcceptEx output buffer
            ::sockaddr* local_addr{};
            ::sockaddr* remote_addr{};
            int         local_len{};
            int         remote_len{};
            constexpr DWORD addr_len = sizeof(::sockaddr_storage) + 16;
            iocp_ctx.get_accept_ex_sockaddrs_fn(
                data->buffer, 0, addr_len, addr_len,
                &local_addr, &local_len,
                &remote_addr, &remote_len);

            // Update the accepted socket's context to inherit listen socket properties
            SOCKET listen_sock = static_cast<SOCKET>(iocp_ctx.native_handle(io->id));
            ::setsockopt(data->accept_socket, SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT,
                         reinterpret_cast<const char*>(&listen_sock), sizeof(SOCKET));

            // Associate the accepted socket with the IOCP
            HANDLE assoc = ::CreateIoCompletionPort(
                reinterpret_cast<HANDLE>(data->accept_socket),
                iocp_ctx.iocp_handle, 0, 0);
            if (assoc == NULL) {
                ::closesocket(data->accept_socket);
                data->accept_socket = INVALID_SOCKET;
                io->error(::std::error_code(
                    static_cast<int>(::GetLastError()),
                    ::std::system_category()));
                return submit_result::error;
            }

            // Store results in the operation tuple
            ::std::get<0>(*aop) = endpoint(remote_addr, static_cast<::socklen_t>(remote_len));
            ::std::get<1>(*aop) = static_cast<::socklen_t>(remote_len);
            ::std::get<2>(*aop) = ctx.make_socket(
                static_cast<native_handle_type>(data->accept_socket));
            data->accept_socket = INVALID_SOCKET; // ownership transferred

            io->complete();
            return submit_result::ready;
        };

        constexpr DWORD addr_len = sizeof(::sockaddr_storage) + 16;
        DWORD           bytes{};
        BOOL ok = accept_ex_fn(
            listen_socket,
            data->accept_socket,
            data->buffer,
            0,          // receive data length (no inline receive)
            addr_len,   // local address length
            addr_len,   // remote address length
            &bytes,
            &data->overlapped);

        if (!ok) {
            int err = ::WSAGetLastError();
            if (err != ERROR_IO_PENDING) {
                op->error(::std::error_code(err, ::std::system_category()));
                return submit_result::error;
            }
        }

        ++outstanding;
        return submit_result::submit;
    }

    auto connect(connect_operation* op) -> submit_result override {
        auto         handle = static_cast<SOCKET>(native_handle(op->id));
        const auto&  ep     = ::std::get<0>(*op);

        // ConnectEx requires the socket to be bound first
        ::sockaddr_storage bind_addr{};
        bind_addr.ss_family = ep.data()->sa_family;
        ::socklen_t bind_len = (bind_addr.ss_family == AF_INET)
            ? static_cast<::socklen_t>(sizeof(::sockaddr_in))
            : static_cast<::socklen_t>(sizeof(::sockaddr_in6));

        if (::bind(handle, reinterpret_cast<const ::sockaddr*>(&bind_addr), bind_len)
            == SOCKET_ERROR) {
            int err = ::WSAGetLastError();
            if (err != WSAEINVAL) { // WSAEINVAL = already bound, which is fine
                op->error(::std::error_code(err, ::std::system_category()));
                return submit_result::error;
            }
        }

        auto* data       = new iocp_overlapped{};
        data->completion = op;
        op->extra = io_base::extra_t(data,
            [](void* p) { delete static_cast<iocp_overlapped*>(p); });

        op->work = [](context_base& ctx, io_base* io) -> submit_result {
            auto* data = static_cast<iocp_overlapped*>(io->extra.get());
            if (data->result < 0) {
                int err = -data->result;
                if (err == static_cast<int>(ERROR_OPERATION_ABORTED)) {
                    io->cancel();
                    return submit_result::ready;
                }
                io->error(::std::error_code(err, ::std::system_category()));
                return submit_result::error;
            }

            // Finalize the connection
            auto h = static_cast<SOCKET>(ctx.native_handle(io->id));
            ::setsockopt(h, SOL_SOCKET, SO_UPDATE_CONNECT_CONTEXT, NULL, 0);

            io->complete();
            return submit_result::ready;
        };

        DWORD bytes{};
        BOOL  ok = connect_ex_fn(
            handle,
            ep.data(),
            ep.size(),
            NULL, 0, // no send data
            &bytes,
            &data->overlapped);

        if (!ok) {
            int err = ::WSAGetLastError();
            if (err != ERROR_IO_PENDING) {
                op->error(::std::error_code(err, ::std::system_category()));
                return submit_result::error;
            }
        }

        ++outstanding;
        return submit_result::submit;
    }

    auto receive(receive_operation* op) -> submit_result override {
        auto  handle = static_cast<SOCKET>(native_handle(op->id));
        auto& msg    = ::std::get<0>(*op);
        auto& rec    = sockets[op->id];

        // Set non-blocking for speculative read
        if (rec.blocking) {
            u_long nonblocking = 1;
            if (::ioctlsocket(handle, FIONBIO, &nonblocking) == 0)
                rec.blocking = false;
        }

        // Speculative read -- skip IOCP if data is already available
        if (!rec.blocking && msg.msg_iov && msg.msg_iovlen > 0) {
            int n = ::recv(handle,
                           static_cast<char*>(msg.msg_iov[0].iov_base),
                           static_cast<int>(msg.msg_iov[0].iov_len), 0);
            if (n >= 0) {
                ::std::get<2>(*op) = static_cast<::std::size_t>(n);
                op->complete();
                return submit_result::ready;
            }
            int err = ::WSAGetLastError();
            if (err == WSAECONNRESET) {
                ::std::get<2>(*op) = 0u;
                op->complete();
                return submit_result::ready;
            }
            if (err != WSAEWOULDBLOCK) {
                op->error(::std::error_code(err, ::std::system_category()));
                return submit_result::error;
            }
            // WSAEWOULDBLOCK -- fall through to IOCP path
        }

        auto* data       = new iocp_io_data{};
        data->completion = op;
        data->flags      = 0;

        // Convert iovec to WSABUF
        if (msg.msg_iov && msg.msg_iovlen > 0) {
            data->wsabuf.buf = static_cast<CHAR*>(msg.msg_iov[0].iov_base);
            data->wsabuf.len = static_cast<ULONG>(msg.msg_iov[0].iov_len);
        }

        op->extra = io_base::extra_t(data,
            [](void* p) { delete static_cast<iocp_io_data*>(p); });

        op->work = [](context_base&, io_base* io) -> submit_result {
            auto* data = static_cast<iocp_io_data*>(io->extra.get());
            auto* rop  = static_cast<receive_operation*>(io);

            if (data->result < 0) {
                int err = -data->result;
                if (err == static_cast<int>(ERROR_OPERATION_ABORTED)) {
                    io->cancel();
                    return submit_result::ready;
                }
                if (err == WSAECONNRESET ||
                    err == static_cast<int>(ERROR_NETNAME_DELETED)) {
                    ::std::get<2>(*rop) = 0u;
                    io->complete();
                    return submit_result::ready;
                }
                io->error(::std::error_code(err, ::std::system_category()));
                return submit_result::error;
            }

            ::std::get<2>(*rop) = static_cast<::std::size_t>(data->result);
            io->complete();
            return submit_result::ready;
        };

        DWORD bytes{};
        int   rc = ::WSARecv(
            handle,
            &data->wsabuf, 1,
            &bytes,
            &data->flags,
            &data->overlapped,
            NULL);

        if (rc == SOCKET_ERROR) {
            int err = ::WSAGetLastError();
            if (err != WSA_IO_PENDING) {
                op->error(::std::error_code(err, ::std::system_category()));
                return submit_result::error;
            }
        }

        ++outstanding;
        return submit_result::submit;
    }

    auto send(send_operation* op) -> submit_result override {
        auto  handle = static_cast<SOCKET>(native_handle(op->id));
        auto& msg    = ::std::get<0>(*op);

        auto* data       = new iocp_io_data{};
        data->completion = op;

        // Convert iovec to WSABUF
        if (msg.msg_iov && msg.msg_iovlen > 0) {
            data->wsabuf.buf = static_cast<CHAR*>(msg.msg_iov[0].iov_base);
            data->wsabuf.len = static_cast<ULONG>(msg.msg_iov[0].iov_len);
        }

        op->extra = io_base::extra_t(data,
            [](void* p) { delete static_cast<iocp_io_data*>(p); });

        op->work = [](context_base&, io_base* io) -> submit_result {
            auto* data = static_cast<iocp_io_data*>(io->extra.get());
            auto* sop  = static_cast<send_operation*>(io);

            if (data->result < 0) {
                int err = -data->result;
                if (err == static_cast<int>(ERROR_OPERATION_ABORTED)) {
                    io->cancel();
                    return submit_result::ready;
                }
                if (err == WSAECONNRESET ||
                    err == static_cast<int>(ERROR_NETNAME_DELETED)) {
                    ::std::get<2>(*sop) = 0u;
                    io->complete();
                    return submit_result::ready;
                }
                io->error(::std::error_code(err, ::std::system_category()));
                return submit_result::error;
            }

            ::std::get<2>(*sop) = static_cast<::std::size_t>(data->result);
            io->complete();
            return submit_result::ready;
        };

        DWORD bytes{};
        DWORD flags = static_cast<DWORD>(::std::get<1>(*op));
        int   rc    = ::WSASend(
            handle,
            &data->wsabuf, 1,
            &bytes,
            flags,
            &data->overlapped,
            NULL);

        if (rc == SOCKET_ERROR) {
            int err = ::WSAGetLastError();
            if (err != WSA_IO_PENDING) {
                op->error(::std::error_code(err, ::std::system_category()));
                return submit_result::error;
            }
        }

        ++outstanding;
        return submit_result::submit;
    }

    auto resume_at(resume_at_operation* op) -> submit_result override {
        if (::std::chrono::system_clock::now() < ::std::get<0>(*op)) {
            timeouts.insert(op);
            return submit_result::submit;
        }
        op->complete();
        return submit_result::ready;
    }
};

} // namespace beman::net::detail

// ----------------------------------------------------------------------------

#endif
