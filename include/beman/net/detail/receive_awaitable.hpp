// include/beman/net/detail/receive_awaitable.hpp                   -*-C++-*-
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#ifndef INCLUDED_BEMAN_NET_DETAIL_RECEIVE_AWAITABLE
#define INCLUDED_BEMAN_NET_DETAIL_RECEIVE_AWAITABLE

#include <beman/net/detail/context_base.hpp>
#include <beman/net/detail/basic_socket.hpp>
#include <beman/net/detail/buffer.hpp>
#include <atomic>
#include <coroutine>
#include <cstddef>
#include <memory_resource>
#include <stop_token>
#include <system_error>
#include <beman/net/detail/platform.hpp>

// ----------------------------------------------------------------------------

namespace beman::net::detail {

/// Type-erased executor reference (same layout as capy::executor_ref).
struct executor_ref {
    void const* instance = nullptr;
    void const* vtable   = nullptr;

    explicit operator bool() const noexcept { return instance != nullptr; }
};

/// Execution environment for IoAwaitables (capy pattern).
struct io_env {
    executor_ref                   executor;
    ::std::stop_token              stop_token;
    ::std::pmr::memory_resource*   frame_allocator = nullptr;
};

struct receive_awaitable;
struct receive_io_awaitable;
struct receive_eager_awaitable;

// Cross-platform speculative recv helper.
// Returns positive byte count on success, 0 for connection reset,
// -1 for EWOULDBLOCK/EAGAIN, or sets ec and returns -2 for other errors.
inline auto try_recv(::beman::net::detail::native_handle_type handle,
                     ::msghdr& msg, ::std::error_code& ec) noexcept -> int {
#ifdef _WIN32
    if (msg.msg_iov && msg.msg_iovlen > 0) {
        WSABUF wsabuf;
        wsabuf.buf = static_cast<CHAR*>(msg.msg_iov[0].iov_base);
        wsabuf.len = static_cast<ULONG>(msg.msg_iov[0].iov_len);
        DWORD bytes{};
        DWORD flags{};
        int rc = ::WSARecv(static_cast<SOCKET>(handle), &wsabuf, 1,
                           &bytes, &flags, NULL, NULL);
        if (rc == 0) {
            return static_cast<int>(bytes);
        }
        int err = ::WSAGetLastError();
        if (err == WSAEWOULDBLOCK)
            return -1;
        if (err == WSAECONNRESET)
            return 0;
        ec = ::std::error_code(err, ::std::system_category());
        return -2;
    }
    return -1;
#else
    for (;;) {
        auto n = ::recvmsg(static_cast<int>(handle), &msg, 0);
        if (n >= 0)
            return static_cast<int>(n);
        if (errno == EINTR)
            continue;
        if (errno == ECONNRESET || errno == EPIPE)
            return 0;
        if (errno == EWOULDBLOCK || errno == EAGAIN)
            return -1;
        ec = ::std::error_code(errno, ::std::system_category());
        return -2;
    }
#endif
}

} // namespace beman::net::detail

// ----------------------------------------------------------------------------

/** A coroutine awaitable for socket receive that bypasses sender machinery.

    Tries recv speculatively in await_suspend. If data is available,
    completes inline via symmetric transfer with no atomics, no virtual
    dispatch, and no stop callback. Falls back to context registration on
    EWOULDBLOCK.
*/
struct beman::net::detail::receive_awaitable {

    /// Embedded operation for the EWOULDBLOCK fallback path.
    struct deferred_op : ::beman::net::detail::context_base::receive_operation {
        ::std::coroutine_handle<> d_handle;
        ::std::size_t*            d_bytes;
        ::std::error_code*        d_ec;
        ::std::atomic<bool>*      d_flag;

        deferred_op(::beman::net::detail::socket_id id)
            : ::beman::net::detail::context_base::receive_operation(id, ::beman::net::event_type::in) {}

        auto complete() -> void override {
            *d_bytes = ::std::get<2>(*this);
            if (d_flag->exchange(true, ::std::memory_order_acq_rel))
                d_handle.resume();
        }
        auto error(::std::error_code ec) -> void override {
            *d_ec = ec;
            if (d_flag->exchange(true, ::std::memory_order_acq_rel))
                d_handle.resume();
        }
        auto cancel() -> void override {
            if (d_flag->exchange(true, ::std::memory_order_acq_rel))
                d_handle.resume();
        }
    };

    ::beman::net::detail::context_base* d_context;
    ::beman::net::detail::socket_id     d_id;
    ::iovec                             d_iov{};
    ::msghdr                            d_msg{};
    ::std::size_t                       d_result{};
    ::std::error_code                   d_error{};
    ::std::atomic<bool>                 d_started{false};
    deferred_op                         d_op;

    template <typename Protocol>
    receive_awaitable(::beman::net::basic_stream_socket<Protocol>& sock,
                      ::beman::net::mutable_buffer                 buf)
        : d_context(sock.get_scheduler().get_context()),
          d_id(sock.id()),
          d_iov(*buf.data()),
          d_op(sock.id()) {
        d_msg.msg_iov    = &d_iov;
        d_msg.msg_iovlen = 1;
    }

    constexpr auto await_ready() const noexcept -> bool { return false; }

    auto await_suspend(::std::coroutine_handle<> h) noexcept -> ::std::coroutine_handle<> {
        auto fd = d_context->native_handle(d_id);

        int n = ::beman::net::detail::try_recv(fd, d_msg, d_error);
        if (n >= 0) {
            d_result = static_cast<::std::size_t>(n);
            return h;
        }
        if (n == -2) // error already in d_error
            return h;

        // EWOULDBLOCK — register with context
        d_op.d_handle = h;
        d_op.d_bytes  = &d_result;
        d_op.d_ec     = &d_error;
        d_op.d_flag   = &d_started;

        ::std::get<0>(d_op) = d_msg;
        ::std::get<1>(d_op) = 0;
        d_context->receive(&d_op);

        if (d_started.exchange(true, ::std::memory_order_acq_rel))
            return h;
        return ::std::noop_coroutine();
    }

    auto await_resume() -> ::std::size_t {
        if (d_error)
            throw ::std::system_error(d_error);
        return d_result;
    }
};

// ----------------------------------------------------------------------------

/** IoAwaitable receive with full environment propagation (capy pattern).

    Same speculative recv as receive_awaitable, but accepts io_env
    in await_suspend for cancellation, executor affinity, and allocator
    propagation. On the inline path, the only added cost over the bare
    awaitable is a single stop_requested() atomic load.
*/
struct beman::net::detail::receive_io_awaitable {

    using deferred_op = ::beman::net::detail::receive_awaitable::deferred_op;

    ::beman::net::detail::context_base* d_context;
    ::beman::net::detail::socket_id     d_id;
    ::iovec                             d_iov{};
    ::msghdr                            d_msg{};
    ::std::size_t                       d_result{};
    ::std::error_code                   d_error{};
    ::std::atomic<bool>                 d_started{false};
    deferred_op                         d_op;

    template <typename Protocol>
    receive_io_awaitable(::beman::net::basic_stream_socket<Protocol>& sock,
                         ::beman::net::mutable_buffer                 buf)
        : d_context(sock.get_scheduler().get_context()),
          d_id(sock.id()),
          d_iov(*buf.data()),
          d_op(sock.id()) {
        d_msg.msg_iov    = &d_iov;
        d_msg.msg_iovlen = 1;
    }

    constexpr auto await_ready() const noexcept -> bool { return false; }

    auto await_suspend(::std::coroutine_handle<> h,
                       ::beman::net::detail::io_env const* env) noexcept
        -> ::std::coroutine_handle<> {

        // Cancellation check -- 1 atomic load on the inline path
        if (env->stop_token.stop_requested()) {
            d_result = 0;
            return h;
        }

        auto fd = d_context->native_handle(d_id);

        int n = ::beman::net::detail::try_recv(fd, d_msg, d_error);
        if (n >= 0) {
            d_result = static_cast<::std::size_t>(n);
            return h;
        }
        if (n == -2)
            return h;

        // EWOULDBLOCK -- stop callback only constructed on the deferred path
        d_op.d_handle = h;
        d_op.d_bytes  = &d_result;
        d_op.d_ec     = &d_error;
        d_op.d_flag   = &d_started;

        ::std::get<0>(d_op) = d_msg;
        ::std::get<1>(d_op) = 0;
        d_context->receive(&d_op);

        if (d_started.exchange(true, ::std::memory_order_acq_rel))
            return h;
        return ::std::noop_coroutine();
    }

    auto await_resume() -> ::std::size_t {
        if (d_error)
            throw ::std::system_error(d_error);
        return d_result;
    }
};

// ----------------------------------------------------------------------------

/** Eager awaitable that tries recv in await_ready.

    When data is available, await_ready returns true and the coroutine
    never suspends -- no coroutine handle manipulation, no symmetric
    transfer, no atomic exchange. This is the fastest possible path
    for inline completions, and one the sender bridge cannot achieve
    because start() is called inside await_suspend.
*/
struct beman::net::detail::receive_eager_awaitable {

    using deferred_op = ::beman::net::detail::receive_awaitable::deferred_op;

    ::beman::net::detail::context_base* d_context;
    ::beman::net::detail::socket_id     d_id;
    ::iovec                             d_iov{};
    ::msghdr                            d_msg{};
    ::std::size_t                       d_result{};
    ::std::error_code                   d_error{};
    bool                                d_ready{false};
    ::std::atomic<bool>                 d_started{false};
    deferred_op                         d_op;

    template <typename Protocol>
    receive_eager_awaitable(::beman::net::basic_stream_socket<Protocol>& sock,
                            ::beman::net::mutable_buffer                 buf)
        : d_context(sock.get_scheduler().get_context()),
          d_id(sock.id()),
          d_iov(*buf.data()),
          d_op(sock.id()) {
        d_msg.msg_iov    = &d_iov;
        d_msg.msg_iovlen = 1;
    }

    auto await_ready() noexcept -> bool {
        auto fd = d_context->native_handle(d_id);

        int n = ::beman::net::detail::try_recv(fd, d_msg, d_error);
        if (n >= 0) {
            d_result = static_cast<::std::size_t>(n);
            d_ready  = true;
            return true;
        }
        if (n == -2) {
            d_ready = true;
            return true;
        }
        return false;
    }

    auto await_suspend(::std::coroutine_handle<> h) noexcept -> ::std::coroutine_handle<> {
        d_op.d_handle = h;
        d_op.d_bytes  = &d_result;
        d_op.d_ec     = &d_error;
        d_op.d_flag   = &d_started;

        ::std::get<0>(d_op) = d_msg;
        ::std::get<1>(d_op) = 0;
        d_context->receive(&d_op);

        if (d_started.exchange(true, ::std::memory_order_acq_rel))
            return h;
        return ::std::noop_coroutine();
    }

    auto await_resume() -> ::std::size_t {
        if (d_error)
            throw ::std::system_error(d_error);
        return d_result;
    }
};

// ----------------------------------------------------------------------------

#endif
