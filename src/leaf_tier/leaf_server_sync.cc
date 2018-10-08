/* Author: Akshitha Sriraman
   Ph.D. Candidate at the University of Michigan - Ann Arbor*/

#include <iostream>
#include <memory>
#include <string>
#include <sys/time.h>
#include <grpc++/grpc++.h>

#include "helper_files/timing.h"
#include "../protoc_files/leaf.grpc.pb.h"

#define NODEBUG

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using leaf::UtilRequest;
using leaf::LeafRequest;
using leaf::UtilResponse;
using leaf::LeafResponse;
using leaf::LeafService;

std::string ip_port = "";
std::mutex leaf_mutex;

// Logic and data behind the server's behavior.
class LeafServiceImpl final : public LeafService::Service {
    Status Leaf(ServerContext* context, const LeafRequest* request,
            LeafResponse* reply) override {
        //std::cout << leaf_server_number << " b " << GetTimeInMicro() << std::endl;
        uint64_t begin = GetTimeInMicro();
        uint64_t request_id = request->request_id();
        /* If the index server is asking for util info,
           it means the time period has expired, so 
           the leaf must read /proc/stat to provide user, system, io, and idle times.*/
        //printf("%p b %lu\n", request, GetTimeInMicro());
#ifndef NODEBUG
        std::cout << "before util\n";
#endif
        if(request->util_request().util_request())
        {
            uint64_t user_time = 0, system_time = 0, io_time = 0, idle_time = 0;
            GetCpuTimes(&user_time,
                    &system_time,
                    &io_time,
                    &idle_time);
            reply->mutable_util_response()->set_user_time(user_time);
            reply->mutable_util_response()->set_system_time(system_time);
            reply->mutable_util_response()->set_io_time(io_time);
            reply->mutable_util_response()->set_idle_time(idle_time);
            reply->mutable_util_response()->set_util_present(true);
        }
#ifndef NODEBUG
        std::cout << "after util\n";
#endif
        uint64_t start_time = 0, end_time = 0;

        // Unpack received queries here.
#ifndef NODEBUG
        std::cout << "after unpack\n";
#endif

        // Perform leaf computations here.

        // Pack leaf response here.

#ifndef NODEBUG
        std::cout << "setting status as ok\n";
#endif
        reply->set_leaf_time(GetTimeInMicro() - begin);
        return Status::OK;
    }
};

void RunServer() {
    std::string server_address(ip_port);
    LeafServiceImpl service;

    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    try
    {
        builder.AddListeningPort(server_address, 
                grpc::InsecureServerCredentials());
    } catch(...) {
        CHECK(false, "ERROR: Enter a valid IP address follwed by port number - IP:Port number\n");
    }
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&service);
    // Finally assemble the server.
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;

    /* Once the leaf server is started, it establishes
       a connection with the already running memcached service.*/

    // Wait for the server to shutdown. Note that some other thread must be
    // responsible for shutting down the server for this call to ever return.
    server->Wait();
}

int main(int argc, char** argv) {
    if (argc == 2) {
        try
        {
            ip_port = argv[1];
        }
        catch(...)
        {
            CHECK(false, "Enter a valid IP and port number\n");
        }
    } else {
        CHECK(false, "Format: ./<leaf_server> <IP address:Port Number>\n");
    }

    RunServer();

    return 0;
}
