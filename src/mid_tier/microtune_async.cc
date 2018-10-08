/* Author: Akshitha Sriraman
   Ph.D. Candidate at the University of Michigan - Ann Arbor*/

#include <memory>
#include <omp.h>
#include <iostream>
#include <string>
#include <thread>
#include <unistd.h>

#include <grpc++/grpc++.h>

#include "set_operations/intersection_service/service/helper_files/client_helper.h"
#include "set_operations/union_service/service/helper_files/union_server_helper.h"
#include "set_operations/union_service/service/helper_files/timing.h"
#include "set_operations/union_service/service/helper_files/utils.h"

#define NODEBUG
#define NOLOADMON

#define ASLEEPEPOCH 5000
#define AWAKEEPOCH 5000
#define TMSIZE 3

#define K 10
#define CIRC_BUFFER_SIZE 12

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Status;
using union_service::UnionRequest;
using union_service::UnionResponse;
using union_service::UnionService;

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using intersection::IntersectionRequest;
using intersection::UtilRequest;
using intersection::TimingDataInMicro;
using intersection::UtilResponse;
using intersection::IntersectionResponse;
using intersection::IntersectionService;

// Class declarations here.
class ServerImpl;
class IntersectionServiceClient;

// Function declarations.
void ProcessRequest(UnionRequest &union_request,
        int union_tid,
        uint64_t unique_request_id_value,
        const AsyncTMNames my_tm);

void ProcessResponses(AsyncTMNames my_tm,
        int tm_num);

void LoadMonitor(void);
void SwitchThreadingModel(const int load);

// Variable definitions here.
/* dataset_dim is global so that we can validate query dimensions whenever 
   batches of queries are received.*/
unsigned int union_parallelism = 0, number_of_response_threads = 0, number_of_intersection_servers = 1, dispatch_parallelism = 0;
std::string ip = "localhost", intersection_server_ips_file = "";
std::vector<std::string> intersection_server_ips;
std::mutex tid_mutex, union_tid_mutex, map_mutex;
std::map<AsyncTMNames, std::vector<IntersectionServiceClient*> > intersection_connections;
ServerImpl* server;
int get_profile_stats = 0;
bool first_req = false, kill_signal = false;

/* Server object is global so that the async intersection client
   thread can access it after it has merged all responses.*/
ResponseMap response_count_down_map;

/* Fine grained locking while looking at individual responses from
   multiple intersection servers. Coarse grained locking when we want to add
   or remove an element from the map.*/
std::mutex response_map_mutex, thread_id, intersection_server_id_mutex, map_coarse_mutex;
std::map<uint64_t, std::unique_ptr<std::mutex> > map_fine_mutex;

/* Single cq tied to all sockets associated
   with all intersection servers.*/
CompletionQueue* intersection_cq = new CompletionQueue();

uint64_t num_requests = 0;
uint64_t req_num = 0;
uint64_t global_time = 0;
std::mutex test;

/* Following variables are used by the auto tuner to 
   detect the current system load.*/
float load = 0.0;
// Total requests are automatically initialized to 0.
Atomics* curr_tm = new Atomics();

std::map<AsyncTMNames, TMConfig> all_tms;
std::vector<ThreadSafeFlagWrapper> tm_notify;
std::vector<ThreadSafeFlagWrapper> workers_notify;
std::vector<ThreadSafeFlagWrapper> resp_notify;
std::map<AsyncTMNames, std::vector<mutex_wrapper> > resp_map_mutex;


/* Declaration of dispatch data structures - 
   helps when switching between different TMS.*/
ThreadSafeQueue<DispatchedData*> dispatched_data_queue;
std::mutex dispatched_data_queue_mutex;

/* Thread safe circular buffer that keeps track on
   an event-based approach to understanding inter-arrival times.*/
CircBuffer<uint64_t> circ_buffer(CIRC_BUFFER_SIZE);
std::vector<uint64_t> k_vec(K, 0);
uint64_t req_cnt = 0;
std::mutex k_vec_mutex;

class ServerImpl final {
    public:
        ~ServerImpl() {
            server_->Shutdown();
            // Always shutdown the completion queue after the server.
            cq_->Shutdown();
        }


        // There is no shutdown handling in this code.
        void Run() {
            std::string ip_port = ip;
            std::string server_address(ip_port);

            ServerBuilder builder;
            // Listen on the given address without any authentication mechanism.
            builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
            // Register "service_" as the instance through which we'll communicate with
            // clients. In this case it corresponds to an *asynchronous* service.
            builder.RegisterService(&service_);

            cq_ = builder.AddCompletionQueue();
            // Finally assemble the server.
            server_ = builder.BuildAndStart();
            std::cout << "Server listening on " << server_address << std::endl;

#ifndef NODEBUG
            std::cout << "before launching thread pools for all TMs\n";
#endif

            /* The flag for the first threading model is set, and 
               flags for all other threading models are reset, so that
               only the first TM starts processing reqs in the beginning.*/
            for(int i = 0; i < TMSIZE; i++)
            {
                if (i == 0) {
                    /* Yes, you call set and this sets flag and 
                       sends a notify_all. But, nobody is waiting for 
                       notifications at this point - so it only just
                       results in some spurious notification msgs.*/
                    tm_notify[i].Set();
                    workers_notify[i].Set();
                    resp_notify[i].Set();
                } else {
                    tm_notify[i].Reset();
                    workers_notify[i].Reset();
                    resp_notify[i].Reset();
                }
            }


            // Launch thread pools for all threading models.
            int tm_num = 0;

            for (std::map<AsyncTMNames, struct TMConfig>::iterator it = all_tms.begin(); it != all_tms.end(); ++it)
            {
                AsyncTMNames my_tm = it->first;
                for(int k = 0; k < it->second.num_workers; k++)
                {
                    it->second.worker_thread_pool.emplace_back(std::thread(&ServerImpl::Dispatch, this, k, my_tm, tm_num));
                }
                tm_num++;
            }
            tm_num = 0;

            for (std::map<AsyncTMNames, struct TMConfig>::iterator it = all_tms.begin(); it != all_tms.end(); ++it)
            {
                AsyncTMNames my_tm = it->first;
                for(int k = 0; k < it->second.num_resps; k++)
                {
                    it->second.resp_thread_pool.emplace_back(std::thread(ProcessResponses, my_tm, tm_num));
                }
                tm_num++;
            }

            tm_num = 0;
            for (std::map<AsyncTMNames, struct TMConfig>::iterator it = all_tms.begin(); it != all_tms.end(); ++it)
            {
                AsyncTMNames my_tm = it->first;
                for(int j = 0; j < it->second.num_inline; j++)
                {
                    it->second.inline_thread_pool.emplace_back(std::thread(&ServerImpl::HandleRpcs, this, j, my_tm, tm_num));
                }

#ifndef NODEBUG
                std::cout << "launched threads for HandleRpcs\n";
#endif
                tm_num++;
            }
            CHECK((TMSIZE == tm_num), "Too many/few TMs\n");

#ifndef NODEBUG
            std::cout << "first threading model handles reqs first. Rest block.\n";
#endif



#ifndef NODEBUG
            std::cout << "before invoking join for all thread pools for all TMs\n";
#endif

            for (std::map<AsyncTMNames, struct TMConfig>::iterator it = all_tms.begin(); it != all_tms.end(); ++it)
            {
                for(int j = 0; j < it->second.num_inline; j++)
                {
                    it->second.inline_thread_pool[j].join();
                }
                for(int k = 0; k < it->second.num_workers; k++)
                {
                    it->second.worker_thread_pool[k].join();
                }
                for(int k = 0; k < it->second.num_resps; k++)
                {
                    it->second.resp_thread_pool[k].join();
                }
            }

        }

        void Finish(uint64_t unique_request_id,
                UnionResponse* union_reply)
        {
            CallData* call_data_req_to_finish = (CallData*) unique_request_id;
            call_data_req_to_finish->Finish(union_reply);
        }

    private:
        // Class encompasing the state and logic needed to serve a request.
        class CallData {
            public:
                // Take in the "service" instance (in this case representing an asynchronous
                // server) and the completion queue "cq" used for asynchronous communication
                // with the gRPC runtime.
                CallData(UnionService::AsyncService* service, ServerCompletionQueue* cq)
                    : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
                        // Invoke the serving logic right away.
                        int union_tid = 0;
                        AsyncTMNames my_tm = aip1_0_1;
                        Proceed(union_tid, my_tm);
                    }

                void Proceed(int union_tid,
                        AsyncTMNames my_tm) {
                    if (status_ == CREATE) {
                        // Make this instance progress to the PROCESS state.
                        status_ = PROCESS;

                        // As part of the initial CREATE state, we *request* that the system
                        // start processing SayHello requests. In this request, "this" acts are
                        // the tag uniquely identifying the request (so that different CallData
                        // instances can serve different requests concurrently), in this case
                        // the memory address of this CallData instance.
                        service_->RequestUnion(&ctx_, &union_request_, &responder_, cq_, cq_,
                                this);
                    } else if (status_ == PROCESS) {
                        // Spawn a new CallData instance to serve new clients while we process
                        // the one for this CallData. The instance will deallocate itself as
                        // part of its FINISH state.
                        new CallData(service_, cq_);
                        uint64_t request_id = reinterpret_cast<uintptr_t>(this);
                        // The actual processing.
                        ProcessRequest(union_request_,
                                union_tid,
                                request_id,
                                my_tm);

                        // And we are done! Let the gRPC runtime know we've finished, using the
                        // memory address of this instance as the uniquely identifying tag for
                        // the event.
                    } else {
                        //GPR_ASSERT(status_ == FINISH);
                        // Once in the FINISH state, deallocate ourselves (CallData).
                        delete this;
                    }
                }

                void Finish(UnionResponse* union_reply)
                {
                    status_ = FINISH;
                    //GPR_ASSERT(status_ == FINISH);
                    responder_.Finish(*union_reply, Status::OK, this);
                }
            private:
                // The means of communication with the gRPC runtime for an asynchronous
                // server.
                UnionService::AsyncService* service_;
                // The producer-consumer queue where for asynchronous server notifications.
                ServerCompletionQueue* cq_;
                // Context for the rpc, allowing to tweak aspects of it such as the use
                // of compression, authentication, as well as to send metadata back to the
                // client.
                ServerContext ctx_;

                // What we get from the client.
                UnionRequest union_request_;
                // What we send back to the client.
                UnionResponse union_reply_;

                // The means to get back to the client.
                ServerAsyncResponseWriter<UnionResponse> responder_;

                // Let's implement a tiny state machine with the following states.
                enum CallStatus { CREATE, PROCESS, FINISH };
                CallStatus status_;  // The current serving state.
        };

        /* Function called by thread that is the worker. Network poller 
           hands requests to this worker thread via a 
           producer-consumer style queue.*/
        void Dispatch(int worker_tid,
                AsyncTMNames my_tm,
                int tm_num) {
            /* We need to make sure that we are a thread
               that belongs to the pool of network pollers belonging
               to the current threading model going on.*/
            while(true)
            {
                if (curr_tm->AtomicallyReadAsyncTM() != my_tm) {
                    workers_notify[tm_num].Reset();
#ifndef NODEBUG
                    std::cout << "my tm = " << my_tm << " waiting " << std::endl;
#endif
                    workers_notify[tm_num].Wait();
                }

                /* Continuously spin and keep checking if there is a
                   dispatched request that needs to be processed.*/
                /* As long as there is a request to be processed,
                   process it. Outer while is just to ensure
                   that we keep waiting for a request when there is
                   nothing in the queue.*/
                DispatchedData* dispatched_request = dispatched_data_queue.pop();
                static_cast<CallData*>(dispatched_request->tag)->Proceed(worker_tid, my_tm);
                delete dispatched_request;
            }
        }

        void InvokeAib(int union_tid,
                AsyncTMNames my_tm,
                void* tag,
                bool* ok)
        {
            cq_->Next(&tag, ok);
            LoadMonitor();
#ifndef NODEBUG
            std::cout << "aft sib next\n";
#endif
            //GPR_ASSERT(ok);
            static_cast<CallData*>(tag)->Proceed(union_tid, my_tm);
        }

        void InvokeAip(int union_tid,
                AsyncTMNames my_tm,
                void* tag,
                bool* ok)
        {

            //auto r = union_srv_cqs[my_tm][union_tid]->AsyncNext(&tag, ok, gpr_time_0(GPR_CLOCK_REALTIME));
            auto r = cq_->AsyncNext(&tag, ok, gpr_time_0(GPR_CLOCK_REALTIME));
#ifndef NODEBUG
            std::cout << "aft sip next\n";
#endif
            if (r == ServerCompletionQueue::GOT_EVENT) {
                LoadMonitor();
                static_cast<CallData*>(tag)->Proceed(union_tid, my_tm);
            }
            if (r == ServerCompletionQueue::TIMEOUT)
            {
                return;
            }
        }


        void InvokeAdb(int union_tid,
                AsyncTMNames my_tm,
                void* tag,
                bool* ok)
        {

            cq_->Next(&tag, ok);
            LoadMonitor();
            /* When we have a new request, we create a new object
               to the dispatch queue.*/
            DispatchedData* request_to_be_dispatched = new DispatchedData();
            request_to_be_dispatched->tag = tag;
            dispatched_data_queue.push(request_to_be_dispatched);

        }

        void InvokeAdp(int union_tid,
                AsyncTMNames my_tm,
                void* tag,
                bool* ok)
        {

            auto r = cq_->AsyncNext(&tag, ok, gpr_time_0(GPR_CLOCK_REALTIME));

            if (r == ServerCompletionQueue::GOT_EVENT) {
                LoadMonitor();
                DispatchedData* request_to_be_dispatched = new DispatchedData();
                request_to_be_dispatched->tag = tag;
                dispatched_data_queue.push(request_to_be_dispatched);
            }
            if (r == ServerCompletionQueue::TIMEOUT) return;

        }


        // This can be run in multiple threads if needed.
        void HandleRpcs(int union_tid, AsyncTMNames my_tm, int tm_num) {
            // Spawn a new CallData instance to serve new clients.
            //new CallData(&service_, union_srv_cqs[my_tm][union_tid].get());
            new CallData(&service_, cq_.get());
            void* tag;  // uniquely identifies a request.
            bool ok;
            while (true) {
#ifndef NODEBUG
                std::cout << "HandleRpcs: entered while\n";
#endif

                /* We need to make sure that we are a thread
                   that belongs to the pool of network pollers belonging
                   to the current threading model going on.*/
                if (curr_tm->AtomicallyReadAsyncTM() != my_tm) {
                    tm_notify[tm_num].Reset();
#ifndef NODEBUG
                    std::cout << "my tm = " << my_tm << " waiting " << std::endl;
#endif
                    tm_notify[tm_num].Wait();
                }
#ifndef NODEBUG
                std::cout << "curr tm = " << curr_tm->AtomicallyReadAsyncTM() << " waiting " << std::endl;
#endif

                switch(my_tm)
                {
                    case aip1_0_1:
                        {
                            InvokeAip(union_tid, my_tm, tag, &ok);
                            break;
                        }
                    case adp1_4_1:
                        {
                            InvokeAdp(union_tid, my_tm, tag, &ok);
                            break;
                        }
                    case adb1_4_4:
                        {
                            InvokeAdb(union_tid, my_tm, tag, &ok);
                            break;
                        }
                    default:
                        CHECK(false, "Panic: Unknown threading model detected\n");

                }
            }
        }

        std::unique_ptr<ServerCompletionQueue> cq_;
        UnionService::AsyncService service_;
        std::unique_ptr<Server> server_;
};



/* Declaring intersection client here because the union server must
   invoke the intersection client to send the queries+PointIDs to the intersection server.*/
class IntersectionServiceClient {
    public:
        IntersectionServiceClient(std::shared_ptr<Channel> channel)
            : stub_(IntersectionService::NewStub(channel)) {}
        /* Assambles the client's payload, sends it and presents the response back
           from the server.*/
        void GetPostingLists(const uint32_t intersection_server_id,
                const bool util_present,
                const uint64_t request_id,
                const AsyncTMNames my_tm,
                IntersectionRequest& request_to_intersection) {
            // Declare the set of queries that must be sent.
            // Create RCP request by adding queries, point IDs, and number of NN.
#ifndef NODEBUG
            std::cout << "bef create\n";
#endif
            CreateIntersectionServiceRequest(intersection_server_id,
                    util_present,
                    &request_to_intersection);

            request_to_intersection.set_request_id(request_id);

#ifndef NODEBUG
            std::cout << "after create\n";
#endif
            // Container for the data we expect from the server.
            IntersectionResponse reply;
            // Context for the client. 
            ClientContext context;
            // The actual RPC call.
#ifndef NODEBUG
            std::cout << "before rpc\n";
#endif
            // Call object to store rpc data
            AsyncClientCall* call = new AsyncClientCall;
            // stub_->AsyncSayHello() performs the RPC call, returning an instance to
            // store in "call". Because we are using the asynchronous API, we need to
            // hold on to the "call" instance in order to get updates on the ongoing RPC.
            call->response_reader = stub_->AsyncIntersection(&call->context, request_to_intersection, intersection_cq);
            // Request that, upon completion of the RPC, "reply" be updated with the
            // server's response; "status" with the indication of whether the operation
            // was successful. Tag the request with the memory address of the call object.
            call->response_reader->Finish(&call->reply, &call->status, (void*)call);
        }

        // Loop while listening for completed responses.
        // Prints out the response from the server.
        void AsyncCompleteRpcBlock(AsyncTMNames my_tm,
                int tm_num) {
            void* got_tag;
            bool ok = false;
            intersection_cq->Next(&got_tag, &ok);
            //auto r = cq_.AsyncNext(&got_tag, &ok, gpr_time_0(GPR_CLOCK_REALTIME));
            //if (r == ServerCompletionQueue::TIMEOUT) return;
            //if (r == ServerCompletionQueue::GOT_EVENT) {
            // The tag in this example is the memory location of the call object
            AsyncClientCall* call = static_cast<AsyncClientCall*>(got_tag);

            // Verify that the request was completed successfully. Note that "ok"
            // corresponds solely to the request for updates introduced by Finish().
            //GPR_ASSERT(ok);

            if (call->status.ok())
            {
                uint64_t unique_request_id = call->reply.request_id();
                /* When this is not the last response, we need to decrement the count
                   as well as collect response meta data - knn answer, intersection util, and
                   intersection timing info.
                   When this is the last request, we remove this request from the map and 
                   merge responses from all intersections.*/
                /* Create local DistCalc, BucketTimingInfo, BucketUtil variables,
                   so that this thread can unpack received intersection data into these variables
                   and then grab a lock to append to the response array in the map.*/
                std::vector<Docids> posting_list;
                IntersectionSrvTimingInfo intersection_timing_info;
                IntersectionSrvUtil intersection_util;
                uint64_t start_time = GetTimeInMicro();
                UnpackIntersectionServiceResponse(call->reply,
                        &posting_list,
                        &intersection_timing_info,
                        &intersection_util);
                uint64_t end_time = GetTimeInMicro();
                // Make sure that the map entry corresponding to request id exists.
                map_coarse_mutex.lock();
                try {
                    response_count_down_map.at(unique_request_id);
                } catch( ... ) {
                    CHECK(false, "ERROR: Map entry corresponding to request id does not exist\n");
                }
                map_coarse_mutex.unlock();

                map_fine_mutex[unique_request_id]->lock();
                int intersection_resp_id = response_count_down_map[unique_request_id].responses_recvd;
                //std::vector<unsigned int> bah = knn_answer.GetValueAtIndex(0);
                *(response_count_down_map[unique_request_id].response_data[intersection_resp_id].posting_list) = posting_list;
                //response_count_down_map[unique_request_id].response_data[intersection_resp_id].knn_answer->AddKnnAnswer(knn_answer.GetValueAtIndex(0), 0);
                *(response_count_down_map[unique_request_id].response_data[intersection_resp_id].intersection_srv_timing_info) = intersection_timing_info;
                *(response_count_down_map[unique_request_id].response_data[intersection_resp_id].intersection_srv_util) = intersection_util;
                response_count_down_map[unique_request_id].response_data[intersection_resp_id].intersection_srv_timing_info->unpack_intersection_srv_resp_time = end_time - start_time;
                if (response_count_down_map[unique_request_id].responses_recvd != (number_of_intersection_servers - 1)) {
                    response_count_down_map[unique_request_id].responses_recvd++;
                    map_fine_mutex[unique_request_id]->unlock();
                } else {
                    uint64_t intersection_resp_start_time = response_count_down_map[unique_request_id].union_reply->get_intersection_srv_responses_time();
                    response_count_down_map[unique_request_id].union_reply->set_get_intersection_srv_responses_time(GetTimeInMicro() - intersection_resp_start_time);
                    /* Time to merge all responses received and then 
                       call terminate so that the response can be sent back
                       to the load generator.*/
                    /* We now know that all intersections have responded, hence we can 
                       proceed to merge responses.*/

                    start_time = GetTimeInMicro();
                    MergeAndPack(response_count_down_map[unique_request_id].response_data,
                            number_of_intersection_servers,
                            response_count_down_map[unique_request_id].union_reply);
                    end_time = GetTimeInMicro();
                    response_count_down_map[unique_request_id].union_reply->set_union_time(end_time - start_time);
                    response_count_down_map[unique_request_id].union_reply->set_pack_union_resp_time(end_time - start_time);
                    //response_count_down_map[unique_request_id].union_reply->set_union_time(union_times[unique_request_id]);
                    /* Call server finish for this particular request,
                       and pass the response so that it can be sent
                       by the server to the frontend.*/
                    //map_fine_mutex[unique_request_id]->unlock();
                    try {
                        map_fine_mutex[unique_request_id]->unlock();
                    } catch (...) {
                        CHECK(false, "couldnt unlock\n");
                    }

                    map_coarse_mutex.lock();
                    server->Finish(unique_request_id,
                            response_count_down_map[unique_request_id].union_reply);
                    map_coarse_mutex.unlock();
                }
            } else {
                CHECK(false, "Bucket does not exist\n");
            }
            // Once we're complete, deallocate the call object.
            delete call;
        }

        void AsyncCompleteRpcPoll(AsyncTMNames my_tm,
                int tm_num) {
            void* got_tag;
            bool ok = false;
            auto r = intersection_cq->AsyncNext(&got_tag, &ok, gpr_time_0(GPR_CLOCK_REALTIME));
            if (r == ServerCompletionQueue::TIMEOUT) return;
            if (r == ServerCompletionQueue::GOT_EVENT) {
                // The tag in this example is the memory location of the call object
                AsyncClientCall* call = static_cast<AsyncClientCall*>(got_tag);

                // Verify that the request was completed successfully. Note that "ok"
                // corresponds solely to the request for updates introduced by Finish().
                //GPR_ASSERT(ok);

                if (call->status.ok())
                {
                    uint64_t unique_request_id = call->reply.request_id();
                    /* When this is not the last response, we need to decrement the count
                       as well as collect response meta data - knn answer, intersection util, and
                       intersection timing info.
                       When this is the last request, we remove this request from the map and 
                       merge responses from all intersections.*/
                    /* Create local DistCalc, BucketTimingInfo, BucketUtil variables,
                       so that this thread can unpack received intersection data into these variables
                       and then grab a lock to append to the response array in the map.*/
                    std::vector<Docids> posting_list;
                    IntersectionSrvTimingInfo intersection_timing_info;
                    IntersectionSrvUtil intersection_util;
                    uint64_t start_time = GetTimeInMicro();
                    UnpackIntersectionServiceResponse(call->reply,
                            &posting_list,
                            &intersection_timing_info,
                            &intersection_util);
                    uint64_t end_time = GetTimeInMicro();
                    // Make sure that the map entry corresponding to request id exists.
                    map_coarse_mutex.lock();
                    try {
                        response_count_down_map.at(unique_request_id);
                    } catch( ... ) {
                        CHECK(false, "ERROR: Map entry corresponding to request id does not exist\n");
                    }
                    map_coarse_mutex.unlock();

                    map_fine_mutex[unique_request_id]->lock();
                    int intersection_resp_id = response_count_down_map[unique_request_id].responses_recvd;
                    *(response_count_down_map[unique_request_id].response_data[intersection_resp_id].posting_list) = posting_list;
                    *(response_count_down_map[unique_request_id].response_data[intersection_resp_id].intersection_srv_timing_info) = intersection_timing_info;
                    *(response_count_down_map[unique_request_id].response_data[intersection_resp_id].intersection_srv_util) = intersection_util;
                    response_count_down_map[unique_request_id].response_data[intersection_resp_id].intersection_srv_timing_info->unpack_intersection_srv_resp_time = end_time - start_time;
                    if (response_count_down_map[unique_request_id].responses_recvd != (number_of_intersection_servers - 1)) {
                        response_count_down_map[unique_request_id].responses_recvd++;
                        map_fine_mutex[unique_request_id]->unlock();
                    } else {
                        uint64_t intersection_resp_start_time = response_count_down_map[unique_request_id].union_reply->get_intersection_srv_responses_time();
                        response_count_down_map[unique_request_id].union_reply->set_get_intersection_srv_responses_time(GetTimeInMicro() - intersection_resp_start_time);
                        /* Time to merge all responses received and then 
                           call terminate so that the response can be sent back
                           to the load generator.*/
                        /* We now know that all intersections have responded, hence we can 
                           proceed to merge responses.*/
                        start_time = GetTimeInMicro();
                        MergeAndPack(response_count_down_map[unique_request_id].response_data,
                                number_of_intersection_servers,
                                response_count_down_map[unique_request_id].union_reply);

                    end_time = GetTimeInMicro();
                    response_count_down_map[unique_request_id].union_reply->set_union_time(end_time - start_time);

                    response_count_down_map[unique_request_id].union_reply->set_pack_union_resp_time(end_time - start_time);
                    //response_count_down_map[unique_request_id].union_reply->set_union_time(union_times[unique_request_id]);
                    /* Call server finish for this particular request,
                       and pass the response so that it can be sent
                       by the server to the frontend.*/
                    map_fine_mutex[unique_request_id]->unlock();

                    map_coarse_mutex.lock();
                    server->Finish(unique_request_id,
                            response_count_down_map[unique_request_id].union_reply);
                    map_coarse_mutex.unlock();
                }
            } else {
                std::cout << "intersection_srv does not exist\n" << std::endl;
            }
            // Once we're complete, deallocate the call object.
            delete call;
            }
        }



            private:
        // struct for keeping state and data information
        struct AsyncClientCall {
            // Container for the data we expect from the server.
            IntersectionResponse reply;
            // Context for the client. It could be used to convey extra information to
            // the server and/or tweak certain RPC behaviors.
            ClientContext context;
            // Storage for the status of the RPC upon completion.
            Status status;
            std::unique_ptr<ClientAsyncResponseReader<IntersectionResponse>> response_reader;
        };

        // Out of the passed in Channel comes the stub, stored here, our view of the
        // server's exposed services.
        std::unique_ptr<IntersectionService::Stub> stub_;

        // The producer-consumer queue we use to communicate asynchronously with the
        // gRPC runtime.
        CompletionQueue cq_;
        };

        void ProcessRequest(UnionRequest &union_request, 
                int union_tid,
                const uint64_t unique_request_id_value,
                const AsyncTMNames my_tm)
        {

            if (kill_signal) {
                std::cout << "got kill\n";
                CHECK(false, "Exit signal received\n");
            }
#ifndef NODEBUG
            std::cout << "inside proc req\n";
#endif
            /* Deine the map entry corresponding to this
               unique request.*/
            // Declare the size of the final response that the map must hold.
            map_coarse_mutex.lock();
            ResponseMetaData meta_data;
            response_count_down_map.erase(unique_request_id_value);
            //map_fine_mutex.erase(unique_request_id_value);
            map_fine_mutex.emplace(unique_request_id_value, std::make_unique<std::mutex>());
            //map_fine_mutex[unique_request_id_value] = make_unique<std::mutex>();
            response_count_down_map[unique_request_id_value] = meta_data;
            map_coarse_mutex.unlock();

            map_fine_mutex[unique_request_id_value]->lock();
            if (union_request.kill()) {
                kill_signal = true;
                response_count_down_map[unique_request_id_value].union_reply->set_kill_ack(true);
                return;
            }
            response_count_down_map[unique_request_id_value].responses_recvd = 0;
            response_count_down_map[unique_request_id_value].response_data.resize(number_of_intersection_servers, ResponseData());
            response_count_down_map[unique_request_id_value].union_reply->set_request_id(union_request.request_id());
            //response_count_down_map[unique_request_id_value].union_reply->set_num_inline(all_tms[my_tm].num_inline);
            //response_count_down_map[unique_request_id_value].union_reply->set_num_workers(all_tms[my_tm].num_workers);
            //response_count_down_map[unique_request_id_value].union_reply->set_num_resp(all_tms[my_tm].num_resps);
            //map_fine_mutex[unique_request_id_value]->unlock();

            bool util_present = union_request.util_request().util_request();
            /* If the load generator is asking for util info,
               it means the time period has expired, so 
               the union must read /proc/stat to provide user, system, and io times.*/
            if(util_present)
            {
                uint64_t start = GetTimeInMicro();
                uint64_t user_time = 0, system_time = 0, io_time = 0, idle_time = 0;
                GetCpuTimes(&user_time,
                        &system_time,
                        &io_time,
                        &idle_time);
                //map_fine_mutex[unique_request_id_value]->lock();
                response_count_down_map[unique_request_id_value].union_reply->mutable_util_response()->mutable_union_util()->set_user_time(user_time);
                response_count_down_map[unique_request_id_value].union_reply->mutable_util_response()->mutable_union_util()->set_system_time(system_time);
                response_count_down_map[unique_request_id_value].union_reply->mutable_util_response()->mutable_union_util()->set_io_time(io_time);
                response_count_down_map[unique_request_id_value].union_reply->mutable_util_response()->mutable_union_util()->set_idle_time(idle_time);
                response_count_down_map[unique_request_id_value].union_reply->mutable_util_response()->set_util_present(true);
                response_count_down_map[unique_request_id_value].union_reply->set_update_union_util_time(GetTimeInMicro() - start);
                //map_fine_mutex[unique_request_id_value]->unlock();
            }
            uint64_t start_time = GetTimeInMicro();
            std::vector<Wordids> word_ids;
            IntersectionRequest request_to_intersection;
            UnpackUnionServiceRequest(union_request,
                    &word_ids,
                    &request_to_intersection);

            uint64_t end_time = GetTimeInMicro();
            //map_fine_mutex[unique_request_id_value]->lock();
            response_count_down_map[unique_request_id_value].union_reply->set_unpack_union_req_time((end_time-start_time));
            //map_fine_mutex[unique_request_id_value]->unlock();
            //float points_sent_percent = PercentDataSent(point_ids, queries_size, dataset_size);
            //printf("Amount of dataset sent to intersection server in the form of point IDs = %.5f\n", points_sent_percent);
            //(*response_count_down_map)[unique_request_id_value]->union_reply->set_percent_data_sent(points_sent_percent);

            start_time = GetTimeInMicro();
            //map_fine_mutex[unique_request_id_value]->lock();
            response_count_down_map[unique_request_id_value].union_reply->set_get_intersection_srv_responses_time(GetTimeInMicro());
            //map_fine_mutex[unique_request_id_value]->unlock();


            for(unsigned int i = 0; i < number_of_intersection_servers; i++) {
                int index = (union_tid * number_of_intersection_servers) + i;
                intersection_connections[my_tm][index]->GetPostingLists(i,
                        util_present,
                        unique_request_id_value,
                        my_tm,
                        request_to_intersection);
            }
            try {
                map_fine_mutex[unique_request_id_value]->unlock();
            } catch( ...) {
                CHECK(false, "could not unlock\n");
            }

        }

        void SwitchThreadingModel(const int load)
        {

            AsyncTMNames prev_tm = curr_tm->AtomicallyReadAsyncTM();


            if (load >= 0 && load < 3000) {
                if (prev_tm != aip1_0_1) {
                    std::cout << "aip1_0_1" << std::endl;
                    curr_tm->AtomicallySetAsyncTM(aip1_0_1);
                    tm_notify[0].Set();
                    workers_notify[0].Set();
                    resp_notify[0].Set();
                } else return;
            } else if (load > 100000) {
                if (prev_tm != adp1_4_1) {
                    std::cout << "adp1_4_1" << std::endl;
                    curr_tm->AtomicallySetAsyncTM(adp1_4_1);
                    tm_notify[1].Set();
                    workers_notify[1].Set();
                    resp_notify[1].Set();
                } else return;
            } else if (load >= 3000) {
                if (prev_tm != adb1_4_4) {
                    std::cout << "adb1_4_4" << std::endl;
                    curr_tm->AtomicallySetAsyncTM(adb1_4_4);
                    tm_notify[2].Set();
                    workers_notify[2].Set();
                    resp_notify[2].Set();
                } else return;
            }

        }

        void LoadMonitor()
        {
            if (circ_buffer.size() != CIRC_BUFFER_SIZE) {
                circ_buffer.send(GetTimeInMicro());
                return;
            }
#ifndef NODEBUG
            std::cout << "inside Load monitor\n";
#endif
            // First insert latest event time stamp into the buffer.
            uint64_t curr_time = GetTimeInMicro();
            circ_buffer.send(curr_time);
#ifndef NODEBUG
            std::cout << "sent latest time stamp into circ buffer\n";
#endif

            // Then compute the difference betweent the latest and the last element.
            uint64_t earliest_time =  circ_buffer.receive();
            uint64_t time_diff = curr_time - earliest_time;
#ifndef NODEBUG
            std::cout << "received earliest time stamp from circ buffer\n";
#endif

            // Calculate qps from the buffer size.
            float time_in_sec = (float)time_diff/1000000.0;
            int load = (int)((float)CIRC_BUFFER_SIZE/time_in_sec); 
            load = load/2;
            k_vec_mutex.lock();
            k_vec[req_cnt % K] = load;
            req_cnt++;
            std::sort(k_vec.begin(), k_vec.end());
            SwitchThreadingModel(k_vec[0.5 * K]);
            k_vec_mutex.unlock();
#ifndef NODEBUG
            std::cout << "calculated load to be " << load << std::endl;
#endif
            //SwitchThreadingModel(load);
#ifndef NODEBUG
            std::cout << "called TM switch\n";
#endif
        } 
        /* The request processing thread runs this 
           function. It checks all the intersection socket connections one by
           one to see if there is a response. If there is one, it then
           implements the count down mechanism in the global map.*/
        void ProcessResponses(AsyncTMNames my_tm,
                int tm_num)
        {
            while(true)
            {
                if (curr_tm->AtomicallyReadAsyncTM() != my_tm) {
                    resp_notify[tm_num].Reset();
#ifndef NODEBUG
                    std::cout << "my tm = " << my_tm << " waiting " << std::endl;
#endif
                    resp_notify[tm_num].Wait();
                }

                if (my_tm == adb1_4_4) {
                    intersection_connections[my_tm][0]->AsyncCompleteRpcBlock(my_tm, 
                            tm_num);
                } else {
                    intersection_connections[my_tm][0]->AsyncCompleteRpcPoll(my_tm, 
                            tm_num);
                }
            }
        }

        int main(int argc, char** argv) {
            if (argc == 7) {
                number_of_intersection_servers = atoi(argv[1]);
                intersection_server_ips_file = argv[2];
                ip = argv[3];
                union_parallelism = atoi(argv[4]);
                dispatch_parallelism = atoi(argv[5]);
                number_of_response_threads = atoi(argv[6]);
            } else {
                CHECK(false, "<./union_server> <number of intersection servers> <intersection server ips file> <ip:port number> <union parallelism>\n");
            }
            // Load intersection server IPs into a string vector
            GetIntersectionServerIPs(intersection_server_ips_file, &intersection_server_ips);


#ifndef NODEBUG
            std::cout << "launched load monitor\n";
#endif

            /* Initialize all threading models i.e
               their name, number of threads associated with each thread pool, etc.*/
            InitializeAsyncTMs(TMSIZE, &all_tms);
            tm_notify.resize(TMSIZE);
            workers_notify.resize(TMSIZE);
            resp_notify.resize(TMSIZE);

            // Launch thread pools for all threading models.
            int num_clients = 0;
            std::vector<std::thread> response_threads;
            for (std::map<AsyncTMNames, struct TMConfig>::iterator it = all_tms.begin(); it != all_tms.end(); ++it)
            {
                AsyncTMNames my_tm = it->first;

                if ( (my_tm == aip1_0_1) ) {
                    num_clients = it->second.num_inline;
                } else {
                    num_clients = it->second.num_workers;
                }

#ifndef NODEBUG
                std::cout << "num clients = " << num_clients << std::endl;
#endif


                for(int i = 0; i < num_clients; i++)
                {
                    for(unsigned int j = 0; j < number_of_intersection_servers; j++)
                    {
                        std::string ip = intersection_server_ips[j];
#ifndef NODEBUG
                        std::cout << "emplaced intersection clients\n";
#endif
                        intersection_connections[my_tm].emplace_back(new IntersectionServiceClient(grpc::CreateChannel(
                                        ip, grpc::InsecureChannelCredentials())));
#ifndef NODEBUG
                        std::cout << "emplaced intersection connections\n";
#endif

                    }
                }


            }



#ifndef NODEBUG
            std::cout << "Initialized TMs. Starting run\n";
#endif

            server = new ServerImpl();
            server->Run();

            return 0;
        }
