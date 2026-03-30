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
#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

namespace ex  = ::beman::execution;
namespace net = ::beman::net;

namespace {

constexpr int         iterations = 30000;
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

// ---- socketpair setup -----------------------------------------------------

struct fd_pair {
    int fd[2]{};
    fd_pair() { [[maybe_unused]] int rc = ::socketpair(AF_UNIX, SOCK_STREAM, 0, fd); assert(rc == 0); }
};

struct socket_pair {
    fd_pair                                 fds;
    net::io_context                         context;
    ::beman::net::detail::context_base*     ctx;
    net::ip::tcp::socket                    writer;
    net::ip::tcp::socket                    reader;

    socket_pair() : ctx(context.get_scheduler().get_context()),
                    writer(ctx, ctx->make_socket(fds.fd[0])),
                    reader(ctx, ctx->make_socket(fds.fd[1])) {}

    void fill(::std::size_t bytes) {
        ::std::vector<char> data(bytes, 'x');
        ::std::size_t written = 0;
        while (written < bytes) {
            auto n = ::write(fds.fd[0], data.data() + written, bytes - written);
            assert(n > 0);
            written += static_cast<::std::size_t>(n);
        }
    }
};

// ---- benchmark A: raw recvmsg ---------------------------------------------

auto bench_raw(socket_pair& sp) -> double {
    sp.fill(total);

    char    buf[chunk];
    ::msghdr msg{};
    ::iovec  iov{buf, chunk};
    msg.msg_iov    = &iov;
    msg.msg_iovlen = 1;
    int fd = sp.ctx->native_handle(sp.reader.id());

    ::fcntl(fd, F_SETFL, O_NONBLOCK);

    auto t0 = ::std::chrono::high_resolution_clock::now();
    for (int i = 0; i < iterations; ++i) {
        ::ssize_t n;
        do { n = ::recvmsg(fd, &msg, 0); } while (n < 0 && errno == EINTR);
        assert(n == static_cast<::ssize_t>(chunk));
    }
    auto t1 = ::std::chrono::high_resolution_clock::now();

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
    ::std::cout << "  raw recvmsg:            " << ::std::setw(8) << raw   << " ns/read\n";
    ::std::cout << "  eager awaitable:        " << ::std::setw(8) << eager << " ns/read  (+" << eager - raw << ")\n";
    ::std::cout << "  bare awaitable:         " << ::std::setw(8) << aw    << " ns/read  (+" << aw - raw << ")\n";
    ::std::cout << "  IoAwaitable (no stop):  " << ::std::setw(8) << io_ns << " ns/read  (+" << io_ns - raw << ")\n";
    ::std::cout << "  IoAwaitable (w/ stop):  " << ::std::setw(8) << io_st << " ns/read  (+" << io_st - raw << ")\n";
    ::std::cout << "  sender+receiver:        " << ::std::setw(8) << sndr  << " ns/read  (+" << sndr - raw << ")\n";
}
