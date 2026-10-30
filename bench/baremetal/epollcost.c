// What netcode's Node mode pays per wake beyond uWS: one epoll_wait on
// libxev's epoll fd, nested inside libuv's, with one socket ready.
#include <stdio.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

static long long ns(void) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return t.tv_sec * 1000000000LL + t.tv_nsec;
}

int main(void) {
    int sv[2];
    socketpair(AF_UNIX, SOCK_STREAM, 0, sv);
    write(sv[1], "x", 1); // stays readable: level-triggered, never read

    int inner = epoll_create1(0); // libxev's
    struct epoll_event ev = {.events = EPOLLIN, .data.fd = sv[0]};
    epoll_ctl(inner, EPOLL_CTL_ADD, sv[0], &ev);
    int outer = epoll_create1(0); // libuv's, watching libxev's fd
    struct epoll_event ev2 = {.events = EPOLLIN, .data.fd = inner};
    epoll_ctl(outer, EPOLL_CTL_ADD, inner, &ev2);

    struct epoll_event out[1024];
    const int n = 2000000;
    for (int round = 0; round < 3; round++) {
        long long t = ns();
        for (int i = 0; i < n; i++) epoll_wait(inner, out, 1024, 0);
        long long a = ns() - t;
        t = ns();
        for (int i = 0; i < n; i++) epoll_wait(outer, out, 1024, 0);
        long long b = ns() - t;
        t = ns();
        for (int i = 0; i < n; i++) getppid();
        long long c = ns() - t;
        printf("epoll_wait(libxev fd, 1 ready, 0) %.0f ns | via the outer fd %.0f ns | getppid %.0f ns\n",
               a / (double)n, b / (double)n, c / (double)n);
    }
    return 0;
}
