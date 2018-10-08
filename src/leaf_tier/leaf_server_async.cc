/* Author: Akshitha Sriraman
   Ph.D. Candidate at the University of Michigan - Ann Arbor*/

#include <iostream>
#include <memory>
#include <omp.h>
#include <string>
#include <sys/time.h>
#include <grpc++/grpc++.h>

#include "helper_files/timing.h"
#include "helper_files/utils.h"
#include "../protoc_files/leaf.grpc.pb.h"

#define NODEBUG

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Status;
using leaf::UtilRequest;
using leaf::LeafRequest;
using leaf::UtilResponse;
using leaf::LeafResponse;
using leaf::LeafService;

std::string ip_port = "";
std::mutex leaf_mutex;
unsigned int leaf_srv_parallelism = 1, leaf_srv_no = 0, num_leaf_srvs = 1;


void ProcessRequest(LeafRequest &request,
        LeafResponse* reply)
{
    // Define your leaf functionality here.
    /* If the index server is asking for util info,
       it means the time period has expired, so 
       the leaf must read /proc/stat to provide user, system, io, and idle times.*/
    //printf("%p b %lu\n", request, GetTimeInMicro());
    uint64_t begin = GetTimeInMicro();
#ifndef NODEBUG
    std::cout << "before util\n";
#endif
    if(request.util_request().util_request())
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

    /* Simply copy request id into the reply - this was just a 
       piggyback message.*/
    reply->set_request_id(request.request_id());

    // Unpack mid-tier request here.

#ifndef NODEBUG
    std::cout << "after unpack\n";
#endif

    // Perform leaf compute here.


#ifndef NODEBUG
    std::cout << "after compute\n";
#endif


    // Pack leaf response here.

    reply->set_leaf_time(GetTimeInMicro() - begin);

}

// Logic and data behind the server's behavior.
class ServiceImpl final {
    public:
        ~ServiceImpl() {
            server_->Shutdown();
            // Always shutdown the completion queue after the server.
            cq_->Shutdown();
        }
        // There is no shutdown handling in this code.
        void Run() {
            std::string server_address(ip_port);
            ServerBuilder builder;
            // Listen on the given address without any authentication mechanism.
            try
            {
                builder.AddListeningPort(server_address,
                        grpc::InsecureServerCredentials());
            } catch(...) {
                CHECK(false, "ERROR: Enter a valid IP address follwed by port number - IP:Port number\n");
            }
            // Register "service_" as the instance through which we'll communicate with
            // clients. In this case it corresponds to an *asynchronous* service.
            builder.RegisterService(&service_);
            // Get hold of the completion queue used for the asynchronous communication
            // with the gRPC runtime.
            cq_ = builder.AddCompletionQueue();
            // Finally assemble the server.
            server_ = builder.BuildAndStart();
            std::cout << "Server listening on " << server_address << std::endl;
            // Proceed to the server's main loop.
            if (leaf_srv_parallelism == 1) {
                HandleRpcs();
            }
            omp_set_dynamic(0);
            omp_set_num_threads(leaf_srv_parallelism);
            omp_set_nested(2);
#pragma omp parallel
            {
                HandleRpcs();
            }
        }    
    private:
        // Class encompasing the state and logic needed to serve a request.
        class CallData {
            public:
                // Take in the "service" instance (in this case representing an asynchronous
                // server) and the completion queue "cq" used for asynchronous communication
                // with the gRPC runtime.
                CallData(LeafService::AsyncService* service, ServerCompletionQueue* cq)
                    : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
                        // Invoke the serving logic right away.
                        Proceed();
                    }

                void Proceed() {
                    if (status_ == CREATE) {
                        // Make this instance progress to the PROCESS state.
                        status_ = PROCESS;

                        // As part of the initial CREATE state, we *request* that the system
                        // start processing requests. In this request, "this" acts are
                        // the tag uniquely identifying the request (so that different CallData
                        // instances can serve different requests concurrently), in this case
                        // the memory address of this CallData instance.
                        service_->RequestLeaf(&ctx_, &request_, &responder_, cq_, cq_,
                                this);
                    } else if (status_ == PROCESS) {
                        // Spawn a new CallData instance to serve new clients while we process
                        // the one for this CallData. The instance will deallocate itself as
                        // part of its FINISH state.
                        new CallData(service_, cq_);
                        // The actual processing.
                        ProcessRequest(request_, &reply_);
                        // And we are done! Let the gRPC runtime know we've finished, using the
                        // memory address of this instance as the uniquely identifying tag for
                        // the event.
                        status_ = FINISH;
                        responder_.Finish(reply_, Status::OK, this);
                    } else {
                        //GPR_ASSERT(status_ == FINISH);
                        // Once in the FINISH state, deallocate ourselves (CallData).
                        delete this;
                    }
                }
            private:
                // The means of communication with the gRPC runtime for an asynchronous
                // server.
                LeafService::AsyncService* service_;
                // The producer-consumer queue where for asynchronous server notifications.
                ServerCompletionQueue* cq_;
                // Context for the rpc, allowing to tweak aspects of it such as the use
                // of compression, authentication, as well as to send metadata back to the
                // client.
                ServerContext ctx_;

                // What we get from the client.
                LeafRequest request_;
                // What we send back to the client.
                LeafResponse reply_;

                // The means to get back to the client.
                ServerAsyncResponseWriter<LeafResponse> responder_;

                // Let's implement a tiny state machine with the following states.
                enum CallStatus { CREATE, PROCESS, FINISH };
                CallStatus status_;  // The current serving state.
        };

        // This can be run in multiple threads if needed.
        void HandleRpcs() {
            // Spawn a new CallData instance to serve new clients.
            new CallData(&service_, cq_.get());
            void* tag;  // uniquely identifies a request.
            bool ok;
            while (true) {
                // Block waiting to read the next event from the completion queue. The
                // event is uniquely identified by its tag, which in this case is the
                // memory address of a CallData instance.
                // The return value of Next should always be checked. This return value
                // tells us whether there is any kind of event or cq_ is shutting down.
                cq_->Next(&tag, &ok);
                //GPR_ASSERT(ok);
                static_cast<CallData*>(tag)->Proceed();
            }
        }

        std::unique_ptr<ServerCompletionQueue> cq_;
        LeafService::AsyncService service_;
        std::unique_ptr<Server> server_;
};

int main(int argc, char** argv) {
    if (argc == 5) {
        try
        {
            ip_port = argv[1];
            leaf_srv_parallelism = atoi(argv[2]);
            leaf_srv_no = atoi(argv[3]);
            num_leaf_srvs = atoi(argv[4]);
        }
        catch(...)
        {
            CHECK(false, "Enter a valid IP and port number / num of cores: -1 if you want all cores on the machine/ leaf server number / number of leaf servers in the system\n");
        }
    } else {
        CHECK(false, "Format: ./<leaf_server> <IP address:Port Number> <num of cores: -1 if you want all cores on the machine> <leaf server number> <number of leaf servers in the system>\n");
    }

    if (leaf_srv_parallelism == -1) {
        leaf_srv_parallelism = GetNumProcs();
        std::cout << leaf_srv_parallelism << std::endl;
    }

    ServiceImpl server;
    server.Run();

    return 0;
}
