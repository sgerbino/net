// tests/beman/net/receive_benchmark.pass.cpp                       -*-C++-*-
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include <beman/net/net.hpp>
#include <beman/net/detail/receive_awaitable.hpp>
#include <beman/execution/execution.hpp>
#include <cassert>
#include <chrono>
#include <coroutine>
#include <cstddef>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <stop_token>
#include <system_error>
#include <vector>
#include <beman/net/detail/platform.hpp>
#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#endif

namespace ex  = ::beman::execution;
namespace net = ::beman::net;

namespace {

#ifdef _WIN32
constexpr int           iterations = 5000;
#else
constexpr int           iterations = 30000;
#endif
constexpr ::std::size_t chunk    = 5;
constexpr ::std::size_t total    = iterations * chunk;

// ---- receiver for the sender path (no coroutine) --------------------------

struct size_receiver {
    using receiver_concept = ex::receiver_t;
    ::std::size_t* size;
    auto set_value(::std::size_t sz) && noexcept -> void { *size = sz; }
    auto set_error(::std::error_code) && noexcept -> void {}
    auto set_error(::std::exception_ptr) && noexcept -> void {}
    auto set_stopped() && noexcept -> void {}
};

// ---- minimal coroutine wrapper for the awaitable path ---------------------

struct bench_coro {
    struct promise_type {
        auto initial_suspend() noexcept -> ::std::suspend_never { return {}; }
        auto final_suspend() noexcept -> ::std::suspend_never { return {}; }
        auto get_return_object() -> bench_coro { return {}; }
        void return_void() {}
        void unhandled_exception() { ::std::terminate(); }
    };
};

// ---- wrapper that bridges io_env into an IoAwaitable's two-arg await_suspend

template <typename A>
struct env_wrapper {
    A&                                      aw;
    ::beman::net::detail::io_env const*     env;
    constexpr auto await_ready() const noexcept { return aw.await_ready(); }
    auto await_suspend(::std::coroutine_handle<> h) noexcept { return aw.await_suspend(h, env); }
    auto await_resume() { return aw.await_resume(); }
};

// ---- cross-platform connected socket pair ---------------------------------

struct socket_pair {
    ::beman::net::detail::native_handle_type raw[2]{
        ::beman::net::detail::invalid_handle,
        ::beman::net::detail::invalid_handle
    };
    net::io_context                         context;
    ::beman::net::detail::context_base*     ctx;
    net::ip::tcp::socket                    writer;
    net::ip::tcp::socket                    reader;

    socket_pair() : ctx(context.get_scheduler().get_context()),
                    writer(ctx, ctx->make_socket(make_pair_fds()[0])),
                    reader(ctx, ctx->make_socket(make_pair_fds()[1])) {}

    auto make_pair_fds() -> ::beman::net::detail::native_handle_type* {
        if (raw[0] != ::beman::net::detail::invalid_handle)
            return raw;
#ifdef _WIN32
        WSADATA wsa;
        ::WSAStartup(MAKEWORD(2, 2), &wsa);

        SOCKET listener = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        assert(listener != INVALID_SOCKET);

        ::sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK);
        addr.sin_port        = 0;
        assert(::bind(listener, reinterpret_cast<::sockaddr*>(&addr), sizeof(addr)) == 0);
        assert(::listen(listener, 1) == 0);

        ::sockaddr_in bound{};
        int len = sizeof(bound);
        assert(::getsockname(listener, reinterpret_cast<::sockaddr*>(&bound), &len) == 0);

        SOCKET client = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        assert(client != INVALID_SOCKET);
        assert(::connect(client, reinterpret_cast<::sockaddr*>(&bound), sizeof(bound)) == 0);

        SOCKET accepted = ::accept(listener, nullptr, nullptr);
        assert(accepted != INVALID_SOCKET);
        ::closesocket(listener);

        // Increase buffer sizes so fill() can buffer all test data
        int bufsize = 256 * 1024;
        ::setsockopt(client, SOL_SOCKET, SO_SNDBUF, reinterpret_cast<const char*>(&bufsize), sizeof(bufsize));
        ::setsockopt(accepted, SOL_SOCKET, SO_RCVBUF, reinterpret_cast<const char*>(&bufsize), sizeof(bufsize));

        raw[0] = static_cast<::beman::net::detail::native_handle_type>(client);
        raw[1] = static_cast<::beman::net::detail::native_handle_type>(accepted);
#else
        int fds[2];
        assert(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);
        raw[0] = fds[0];
        raw[1] = fds[1];
#endif
        return raw;
    }

    void fill(::std::size_t bytes) {
        ::std::vector<char> data(bytes, 'x');
        auto handle = ctx->native_handle(writer.id());
        ::std::size_t written = 0;
        while (written < bytes) {
#ifdef _WIN32
            int n = ::send(static_cast<SOCKET>(handle), data.data() + written,
                           static_cast<int>(bytes - written), 0);
#else
            auto n = ::write(static_cast<int>(handle), data.data() + written, bytes - written);
#endif
            assert(n > 0);
            written += static_cast<::std::size_t>(n);
        }
    }
};

// ---- benchmark A: raw recv ------------------------------------------------

auto bench_raw(socket_pair& sp) -> double {
    sp.fill(total);

    char buf[chunk];
    auto handle = sp.ctx->native_handle(sp.reader.id());

#ifdef _WIN32
    u_long nonblocking = 1;
    ::ioctlsocket(static_cast<SOCKET>(handle), FIONBIO, &nonblocking);

    auto t0 = ::std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iterations; ++i) {
        int n;
        do { n = ::recv(static_cast<SOCKET>(handle), buf, static_cast<int>(chunk), 0); }
        while (n < 0 && ::WSAGetLastError() == WSAEINTR);
        assert(n == static_cast<int>(chunk));
    }
    auto t1 = ::std::chrono::high_resolution_clock::now();
#else
    ::fcntl(static_cast<int>(handle), F_SETFL, O_NONBLOCK);

    ::msghdr msg{};
    ::iovec  iov{buf, chunk};
    msg.msg_iov    = &iov;
    msg.msg_iovlen = 1;

    auto t0 = ::std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iterations; ++i) {
        ::ssize_t n;
        do { n = ::recvmsg(static_cast<int>(handle), &msg, 0); } while (n < 0 && errno == EINTR);
        assert(n == static_cast<::ssize_t>(chunk));
    }
    auto t1 = ::std::chrono::high_resolution_clock::now();
#endif

    return ::std::chrono::duration<double, ::std::nano>(t1 - t0).count() / iterations;
}

// ---- benchmark B: sender + manual receiver --------------------------------

auto bench_sender(socket_pair& sp) -> double {
    sp.fill(total);

    char buf[chunk];

    auto t0 = ::std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iterations; ++i) {
        ::std::size_t received = 0;
        auto sndr  = net::async_receive(sp.reader, net::buffer(buf, chunk));
        auto state = ex::connect(::std::move(sndr), size_receiver{&received});
        ex::start(state);
        assert(received == chunk);
    }
    auto t1 = ::std::chrono::high_resolution_clock::now();

    return ::std::chrono::duration<double, ::std::nano>(t1 - t0).count() / iterations;
}

// ---- benchmark C: native awaitable in coroutine ---------------------------

auto bench_awaitable(socket_pair& sp) -> double {
    sp.fill(total);

    char    buf[chunk];
    double  ns_per_read = 0;

    auto run = [&]() -> bench_coro {
        auto t0 = ::std::chrono::high_resolution_clock::now();
        for (int i = 0; i < iterations; ++i) {
            auto n = co_await ::beman::net::detail::receive_awaitable(sp.reader, net::buffer(buf, chunk));
            assert(n == chunk);
        }
        auto t1 = ::std::chrono::high_resolution_clock::now();
        ns_per_read = ::std::chrono::duration<double, ::std::nano>(t1 - t0).count() / iterations;
    };

    run();
    return ns_per_read;
}

// ---- benchmark D: IoAwaitable with empty stop token -----------------------

auto bench_io_awaitable_no_stop(socket_pair& sp) -> double {
    sp.fill(total);

    char   buf[chunk];
    double ns_per_read = 0;

    ::beman::net::detail::io_env env{};

    auto run = [&]() -> bench_coro {
        auto t0 = ::std::chrono::high_resolution_clock::now();
        for (int i = 0; i < iterations; ++i) {
            auto aw = ::beman::net::detail::receive_io_awaitable(sp.reader, net::buffer(buf, chunk));
            auto n  = co_await env_wrapper<decltype(aw)>{aw, &env};
            assert(n == chunk);
        }
        auto t1 = ::std::chrono::high_resolution_clock::now();
        ns_per_read = ::std::chrono::duration<double, ::std::nano>(t1 - t0).count() / iterations;
    };

    run();
    return ns_per_read;
}

// ---- benchmark E: IoAwaitable with active stop token ----------------------

auto bench_io_awaitable_with_stop(socket_pair& sp) -> double {
    sp.fill(total);

    char   buf[chunk];
    double ns_per_read = 0;

    ::std::stop_source           src;
    ::beman::net::detail::io_env env{{}, src.get_token()};

    auto run = [&]() -> bench_coro {
        auto t0 = ::std::chrono::high_resolution_clock::now();
        for (int i = 0; i < iterations; ++i) {
            auto aw = ::beman::net::detail::receive_io_awaitable(sp.reader, net::buffer(buf, chunk));
            auto n  = co_await env_wrapper<decltype(aw)>{aw, &env};
            assert(n == chunk);
        }
        auto t1 = ::std::chrono::high_resolution_clock::now();
        ns_per_read = ::std::chrono::duration<double, ::std::nano>(t1 - t0).count() / iterations;
    };

    run();
    return ns_per_read;
}

// ---- benchmark F: eager awaitable (await_ready speculative read) ----------

auto bench_eager(socket_pair& sp) -> double {
    sp.fill(total);

    char   buf[chunk];
    double ns_per_read = 0;

    auto run = [&]() -> bench_coro {
        auto t0 = ::std::chrono::high_resolution_clock::now();
        for (int i = 0; i < iterations; ++i) {
            auto n = co_await ::beman::net::detail::receive_eager_awaitable(sp.reader, net::buffer(buf, chunk));
            assert(n == chunk);
        }
        auto t1 = ::std::chrono::high_resolution_clock::now();
        ns_per_read = ::std::chrono::duration<double, ::std::nano>(t1 - t0).count() / iterations;
    };

    run();
    return ns_per_read;
}

} // namespace

auto main() -> int {
    socket_pair sp;

    // Warmup
    bench_raw(sp);
    bench_sender(sp);
    bench_awaitable(sp);
    bench_io_awaitable_no_stop(sp);
    bench_io_awaitable_with_stop(sp);
    bench_eager(sp);

    auto raw    = bench_raw(sp);
    auto sndr   = bench_sender(sp);
    auto aw     = bench_awaitable(sp);
    auto io_ns  = bench_io_awaitable_no_stop(sp);
    auto io_st  = bench_io_awaitable_with_stop(sp);
    auto eager  = bench_eager(sp);

    ::std::cout << ::std::fixed << ::std::setprecision(1);
    ::std::cout << "receive benchmark (" << iterations << " iterations, " << chunk << " bytes each)\n\n";
    ::std::cout << "  raw recv:               " << ::std::setw(8) << raw   << " ns/read\n";
    ::std::cout << "  eager awaitable:        " << ::std::setw(8) << eager << " ns/read  (+" << eager - raw << ")\n";
    ::std::cout << "  bare awaitable:         " << ::std::setw(8) << aw    << " ns/read  (+" << aw - raw << ")\n";
    ::std::cout << "  IoAwaitable (no stop):  " << ::std::setw(8) << io_ns << " ns/read  (+" << io_ns - raw << ")\n";
    ::std::cout << "  IoAwaitable (w/ stop):  " << ::std::setw(8) << io_st << " ns/read  (+" << io_st - raw << ")\n";
    ::std::cout << "  sender+receiver:        " << ::std::setw(8) << sndr  << " ns/read  (+" << sndr - raw << ")\n";
}
