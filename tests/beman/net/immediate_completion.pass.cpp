// tests/beman/net/immediate_completion.pass.cpp                    -*-C++-*-
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception

#include <beman/net/net.hpp>
#include <beman/execution/execution.hpp>
#include <cassert>
#include <chrono>
#include <iostream>
#include <string_view>
#include <system_error>
#include <sys/socket.h>
#include <unistd.h>

namespace ex  = ::beman::execution;
namespace net = ::beman::net;

namespace {

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
// via the speculative work() path — no poll() syscall needed.
auto test_socket_immediate_receive() -> void {
    ::std::cout << "test socket immediate receive\n";

    int fds[2];
    assert(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);

    net::io_context context;
    auto*           ctx = context.get_scheduler().get_context();

    net::ip::tcp::socket writer(ctx, ctx->make_socket(fds[0]));
    net::ip::tcp::socket reader(ctx, ctx->make_socket(fds[1]));

    const char msg[] = "abcdefghijklmnopqrstuvwxy";
    assert(::write(fds[0], msg, 25) == 25);

    char          buf[25]    = {};
    ::std::size_t total_read = 0;

    for (int i = 0; i < 5; ++i) {
        ::std::size_t received_size = 0;
        bool          completed     = false;

        auto sndr  = net::async_receive(reader, net::buffer(buf + total_read, 5));
        auto state = ex::connect(::std::move(sndr), size_receiver{&received_size, &completed});
        ex::start(state);

        // Completes inline — no event loop iteration needed
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
