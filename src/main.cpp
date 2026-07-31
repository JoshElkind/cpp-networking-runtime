#include "nmr/server.hpp"

#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <thread>

namespace {
nmr::Server* g_server = nullptr;
void on_sig(int) {
  if (g_server) g_server->stop();
}
}  // namespace

int main(int argc, char** argv) {
  std::uint16_t port = 9000;
  int workers = 2;
  if (argc > 1) port = static_cast<std::uint16_t>(std::atoi(argv[1]));
  if (argc > 2) workers = std::atoi(argv[2]);

  nmr::Server server(port, workers);
  g_server = &server;
  std::signal(SIGINT, on_sig);
  std::signal(SIGTERM, on_sig);
  server.start();
  std::cout << "nmr-broker listening on 127.0.0.1:" << server.port()
            << " workers=" << workers << std::endl;
  while (true) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}
