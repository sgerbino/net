// tests/beman/net/immediate_completion.pass.cpp                    -*-C++-*-
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include <beman/net/net.hpp>
#include <beman/execution/execution.hpp>
#include <cassert>
#include <chrono>
#include <iostream>
#include <string_view>
#include <system_error>
#include <beman/net/detail/platform.hpp>
#ifndef _WIN32
#include <unistd.h>
#endif

namespace ex  = ::beman::execution;
namespace net = ::beman::net;

namespace {

// Cross-platform connected socket pair using TCP loopback.
struct socket_pair {
    ::beman::net::detail::native_handle_type fds[2]{};

    socket_pair() {
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

        fds[0] = static_cast<::beman::net::detail::native_handle_type>(client);
        fds[1] = static_cast<::beman::net::detail::native_handle_type>(accepted);
#else
        int raw[2];
        assert(::socketpair(AF_UNIX, SOCK_STREAM, 0, raw) == 0);
        fds[0] = raw[0];
        fds[1] = raw[1];
#endif
    }

    auto write_all(const void* data, ::std::size_t len) -> void {
        const char* p = static_cast<const char*>(data);
        ::std::size_t written = 0;
        while (written < len) {
#ifdef _WIN32
            int n = ::send(static_cast<SOCKET>(fds[0]), p + written,
                           static_cast<int>(len - written), 0);
#else
            auto n = ::write(fds[0], p + written, len - written);
#endif
            assert(n > 0);
            written += static_cast<::std::size_t>(n);
        }
    }
};

struct size_receiver {
    using receiver_concept = ex::receiver_t;
    ::std::size_t* size;
    bool*          done;
    auto set_value(::std::size_t sz) && noexcept -> void {
        *size = sz;
        *done = true;
    }
    auto set_error(::std::error_code) && noexcept -> void {}
    auto set_error(::std::exception_ptr) && noexcept -> void {}
    auto set_stopped() && noexcept -> void {}
};

// A zero-duration timer completes inline during start() because
// poll_context::resume_at returns submit_result::ready when now >= time_point.
// sync_wait returns without driving the io_context event loop.
auto test_timer_immediate_completion() -> void {
    ::std::cout << "test timer immediate completion\n";
    using namespace ::std::chrono_literals;
    net::io_context context;

    auto result = ex::sync_wait(net::resume_after(context.get_scheduler(), 0s));
    assert(result.has_value());
    assert(context.run() == 0);
}

// Write 25 bytes then drain them with 5 receives of 5 bytes each.
// Data is pre-loaded so each async_receive completes inline during start()
// via the speculative work() path -- no poll()/IOCP syscall needed.
auto test_socket_immediate_receive() -> void {
    ::std::cout << "test socket immediate receive\n";

    socket_pair sp;
    net::io_context context;
    auto*           ctx = context.get_scheduler().get_context();

    net::ip::tcp::socket writer(ctx, ctx->make_socket(sp.fds[0]));
    net::ip::tcp::socket reader(ctx, ctx->make_socket(sp.fds[1]));

    const char msg[] = "abcdefghijklmnopqrstuvwxy";
    sp.write_all(msg, 25);

    char          buf[25]    = {};
    ::std::size_t total_read = 0;

    for (int i = 0; i < 5; ++i) {
        ::std::size_t received_size = 0;
        bool          completed     = false;

        auto sndr  = net::async_receive(reader, net::buffer(buf + total_read, 5));
        auto state = ex::connect(::std::move(sndr), size_receiver{&received_size, &completed});
        ex::start(state);

        // Completes inline -- no event loop iteration needed
        assert(completed);
        assert(received_size == 5);
        total_read += received_size;
    }

    assert(total_read == 25);
    assert(::std::string_view(buf, 25) == "abcdefghijklmnopqrstuvwxy");
    assert(context.run() == 0);
}

} // namespace

auto main() -> int {
    test_timer_immediate_completion();
    test_socket_immediate_receive();
}
