#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>

#include "zetasql/local_service/local_service_grpc.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

namespace zetasql {
namespace local_service {
namespace {

extern "C" void RunServer(int port) {
  std::string server_address_with_port = "localhost:" + std::to_string(port);
  std::cout << "Running server on " << server_address_with_port << "..." << std::endl;

  ZetaSqlLocalServiceGrpcImpl* service =
        new ZetaSqlLocalServiceGrpcImpl();

  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address_with_port, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address_with_port << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}
}
}
}

int main(int argc, char* argv[]) {
  std::string port = argv[1];
  zetasql::local_service::RunServer(std::stoi(port));
  return 0;
}
