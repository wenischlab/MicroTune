/* Author: Akshitha Sriraman
   Ph.D. Candidate at the University of Michigan - Ann Arbor*/

#include <memory>
#include <omp.h>
#include <iostream>
#include <string>
#include <thread>
#include <unistd.h>

#include <grpc++/grpc++.h>

#include "helper_files/mid_tier_server_helper.h"
#include "helper_files/timing.h"
#include "helper_files/utils.h"

#define NODEBUG
#define NOLOADMON

// Define your numbers here.
#define WINDOWSIZE 60
#define ASLEEPEPOCH 1
#define AWAKEEPOCH 1
#define TMSIZE 3

#define K 10
#define CIRC_BUFFER_SIZE 12

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Status;
using mid_tier_service::MidTierRequest;
using mid_tier_service::MidTierResponse;
using mid_tier_service::MidTierService;

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using leaf::LeafRequest;
using leaf::UtilRequest;
using leaf::LeafResponse;
using leaf::LeafService;

// Class declarations here.
class LeafServiceClient;

// Function declarations here.
void LoadMonitor(void);
void SwitchThreadingModel(const int load);

// Variable definitions here.
/* dataset_dim is global so that we can validate query dimensions whenever 
   batches of queries are received.*/
unsigned int number_of_leaf_servers = 1, mid_tier_parallelism = 0, dispatch_parallelism = 0;
std::string ip = "localhost", leaf_server_ips_file = "";
std::vector<std::string> leaf_server_ips;
/* lsh_mid_tier is global so that the server can build/load the mid_tier in the very 
   beginning, before it accepts any queries. Subsequent queries can then use 
   this mid_tier structure already built, to get point IDs.*/
std::mutex tid_mutex, mid_tier_tid_mutex, map_mutex;
std::map<TMNames, std::vector<LeafServiceClient*> > leaf_connections;
bool first_req = false, kill_signal;

std::map<TMNames, std::vector<ThreadSafeQueueReqWrapper> > requests_to_leaf_srv_queue;
std::map<TMNames, std::vector<ThreadSafeQueueRespWrapper> > resp_recvd_from_leaf_srv;

ResponseMap response_map;

uint64_t req_num = 0;
uint64_t global_time = 0;
std::mutex test;

/* Following variables are used by the auto tuner to 
   detect the current system load.*/
int load = 0;
// Total requests are automatically initialized to 0.
Atomics* cnt_req_flag = new Atomics();
Atomics* curr_tm = new Atomics();

std::map<TMNames, TMConfig> all_tms;
std::vector<ThreadSafeFlagWrapper> tm_notify;
std::vector<ThreadSafeFlagWrapper> workers_notify;
std::map<TMNames, std::vector<mutex_wrapper> > resp_map_mutex;


/* Declaration of dispatch data structures - 
   helps when switching between different TMS.*/
ThreadSafeQueue<DispatchedData*> dispatched_data_queue;
std::mutex dispatched_data_queue_mutex;


/* Trial: Adding multiple completion queues, so that each thread has
   its own cq.*/
std::map<TMNames, std::vector<std::unique_ptr<ServerCompletionQueue> > > mid_tier_srv_cqs;

/* Thread safe circular buffer that keeps track on
   an event-based approach to understanding inter-arrival times.*/
CircBuffer<uint64_t> circ_buffer(CIRC_BUFFER_SIZE);
std::vector<uint64_t> k_vec(K, 0);
uint64_t req_cnt = 0;
std::mutex k_vec_mutex;

/* Declaring leaf client here because the mid_tier server must
   invoke the leaf client to send the queries+PointIDs to the leaf server.*/
class LeafServiceClient {
    public:
        LeafServiceClient(std::shared_ptr<Channel> channel)
            : stub_(LeafService::NewStub(channel)) {}
        /* Assamble the client's payload, sends it and presents the response back
           from the server.*/
        // Complete your unction here. Part of ProcessRequest().
        void YourClientFunction(const uint32_t leaf_server_id,
                const bool util_present,
                const uint64_t request_id,
                const int mid_tier_tid,
                const TMNames my_tm,
                LeafRequest request_to_leaf,
                LeafSrvTimingInfo* leaf_timing_info,
                LeafSrvUtil* leaf_util) {
            // Declare the set of queries that must be sent.
            uint64_t start_time, end_time;
            start_time = GetTimeInMicro();
            // Create RCP request by adding queries, point IDs, and number of NN.
#ifndef NODEBUG
            std::cout << "bef create\n";
#endif
            // Create your leaf request here.
            request_to_leaf.set_leaf_server_id(leaf_server_id);
            request_to_leaf.set_request_id(request_id);

#ifndef NODEBUG
            std::cout << "after create\n";
#endif
            // Container for the data we expect from the server.
            LeafResponse reply;
            // Context for the client. 
            ClientContext context;
            // The actual RPC call.
#ifndef NODEBUG
            std::cout << "before rpc\n";
#endif
            start_time = GetTimeInMicro();
            Status status = stub_->Leaf(&context, request_to_leaf, &reply);
            end_time = GetTimeInMicro();

            // Convert received data into suitable form here.

            // Act upon its status.

            if (!status.ok()) {
                std::cout << leaf_server_id << std::endl;
                std::cout << status.error_code() << ": " << status.error_message() << std::endl;
                CHECK(false, "Status is Not OK");
            }
#ifndef NODEBUG
            std::cout << "client: bef pushing resp\n";
#endif

            resp_map_mutex[my_tm][mid_tier_tid].lock();
            int curr_id = ++response_map[request_id].responses_recvd;

            if ((unsigned int)curr_id == number_of_leaf_servers) {
                response_map[request_id].responses_recvd = 0;
                resp_recvd_from_leaf_srv[my_tm][mid_tier_tid].push(true);
            }
            resp_map_mutex[my_tm][mid_tier_tid].unlock();
#ifndef NODEBUG
            std::cout << "pushed final resp\n";
#endif
        }


    private:
        std::unique_ptr<LeafService::Stub> stub_;
};

/* Function gets invoked by each mid_tier server thread,
   leaf server number of times.*/
void LaunchLeafClient(const int id,
        TMNames my_tm)
{
#ifndef NODEBUG
    std::cout << "inside launch\n";
#endif
    while (true)
    {
        ReqToLeafSrv req_to_leaf_srv = requests_to_leaf_srv_queue[my_tm][id].pop();
        leaf_connections[my_tm][id]->YourClientFunction(req_to_leaf_srv.leaf_server_id,
                req_to_leaf_srv.util_present,
                req_to_leaf_srv.request_id,
                req_to_leaf_srv.mid_tier_tid,
                my_tm,
                req_to_leaf_srv.request_to_leaf_srv,
                req_to_leaf_srv.leaf_srv_timing_info,
                req_to_leaf_srv.leaf_srv_util);
#ifndef NODEBUG
        std::cout << "before pushing req\n";
#endif
    }

#ifndef NODEBUG
    std::cout << "eof launch\n";
#endif
}


void ProcessRequest(MidTierRequest &mid_tier_request,
        int mid_tier_tid,
        const uint64_t request_id,
        const TMNames my_tm,
        MidTierResponse* mid_tier_reply) 
{

    if (kill_signal) {
        std::cout << "got kill\n";
        CHECK(false, "Exit signal received\n");
    }
#ifndef NODEBUG
    std::cout << "inside proc req\n";
#endif
    if (mid_tier_request.kill()) {
        kill_signal = true;
        mid_tier_reply->set_kill_ack(true);
        return;   
    }
#ifndef NODEBUG
    std::cout << "after kill\n";
#endif
    mid_tier_reply->set_request_id(mid_tier_request.request_id());
    bool util_present = mid_tier_request.util_request().util_request();
    /* If the load generator is asking for util info,
       it means the time period has expired, so 
       the mid_tier must read /proc/stat to provide user, system, and io times.*/
    uint64_t start = GetTimeInMicro();

    if(util_present)
    {
        uint64_t user_time = 0, system_time = 0, io_time = 0, idle_time = 0;
        GetCpuTimes(&user_time,
                &system_time,
                &io_time,
                &idle_time);
        mid_tier_reply->mutable_util_response()->mutable_mid_tier_util()->set_user_time(user_time);
        mid_tier_reply->mutable_util_response()->mutable_mid_tier_util()->set_system_time(system_time);
        mid_tier_reply->mutable_util_response()->mutable_mid_tier_util()->set_io_time(io_time);
        mid_tier_reply->mutable_util_response()->mutable_mid_tier_util()->set_idle_time(idle_time);
        mid_tier_reply->mutable_util_response()->set_util_present(true);
    }

    mid_tier_reply->set_num_inline(mid_tier_parallelism);
    mid_tier_reply->set_num_workers(dispatch_parallelism);
    mid_tier_reply->set_num_workers(0);
    mid_tier_reply->set_num_resp(0);

    uint64_t start_time = GetTimeInMicro();

    /* Creating request to leaf server here, so that the query can
       be directly copied - prevents another for loop later on.*/
    LeafRequest request_to_leaf;
    // Unpack request sent to mid-tier over here.

#ifndef NODEBUG
    std::cout << "aft unpack\n";
#endif

    /* Invoke leaf client - create separate threads for each leaf client.
       Design choice: thread ID = leaf server ID*/
    struct ThreadArgs* thread_args = new struct ThreadArgs[number_of_leaf_servers];
    struct ReqToLeafSrv* req_to_leaf_srv = new struct ReqToLeafSrv[number_of_leaf_servers];

    start_time = GetTimeInMicro();
    for(unsigned int i = 0; i < number_of_leaf_servers; i++)
    {
        /* Form the request that must be sent to each leaf server. This
           request must then be pushed into a thread-safe produce-consumer
           queue, so that the corresponding thread that is waiting on the
           corresponding queue gets notified of the fact that there is a 
           request that it needs to send to the leaf server via an rpc call.*/
        req_to_leaf_srv[i].mid_tier_tid = mid_tier_tid;
        req_to_leaf_srv[i].util_present = util_present;
        req_to_leaf_srv[i].request_to_leaf_srv = request_to_leaf;
        req_to_leaf_srv[i].leaf_srv_timing_info = &thread_args[i].leaf_srv_timing_info;
        req_to_leaf_srv[i].leaf_srv_util = &thread_args[i].leaf_srv_util;
        req_to_leaf_srv[i].leaf_server_id = i;
        req_to_leaf_srv[i].request_id = request_id;

        response_map[request_id].responses_recvd = 0;

        int index = (mid_tier_tid * number_of_leaf_servers) + i;
        requests_to_leaf_srv_queue[my_tm][index].push(req_to_leaf_srv[i]);
    }
#ifndef NODEBUG
    std::cout << "after lsh\n";
#endif
    uint64_t end_time = GetTimeInMicro();

#ifndef NODEBUG
    std::cout << "after sending reqs to leafs\n";
#endif

    start_time = GetTimeInMicro();
#ifndef NODEBUG
    std::cout << "mid_tier: waiting to pop resp\n";
#endif
    resp_recvd_from_leaf_srv[my_tm][mid_tier_tid].pop();
#ifndef NODEBUG
    std::cout << "mid_tier: popped resp\n";
#endif

#ifndef NODEBUG
    std::cout << "after receiving resp from leafs\n";
#endif

    delete[] req_to_leaf_srv;

    uint64_t create_leaf_srv_req_time = 0, unpack_leaf_srv_resp_time = 0, unpack_leaf_srv_req_time = 0, calculate_leaf_time = 0, pack_leaf_srv_resp_time = 0;
    start_time = GetTimeInMicro();
    Merge(thread_args,
            number_of_leaf_servers,
            &calculate_leaf_time);

    end_time = GetTimeInMicro();

    // Convert K-NN into form suitable for GRPC.
    start_time = GetTimeInMicro();
    PackMidTierServiceResponse(
            thread_args,
            number_of_leaf_servers,
            mid_tier_reply);
    end_time = GetTimeInMicro();
    mid_tier_reply->set_calculate_leaf_time(calculate_leaf_time);
    mid_tier_reply->set_number_of_leaf_servers(number_of_leaf_servers);
#ifndef NODEBUG
    std::cout << "eof procreq\n";
#endif
    delete[] thread_args;
}

class ServerImpl final {
    public:
        ~ServerImpl() {
            server_->Shutdown();
            // Always shutdown the completion queue after the server.
#if 0
            for (std::map<TMNames, struct TMConfig>::iterator it = all_tms.begin(); it != all_tms.end(); ++it)
            {
                for(int i = 0; i < mid_tier_srv_cqs[it->first].size(); i++) 
                {
                    mid_tier_srv_cqs.at(it->first)[i]->Shutdown();
                }
            }
#endif
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

            // Add cqs here.
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
                } else {
                    tm_notify[i].Reset();
                    workers_notify[i].Reset();
                }
            }


            // Launch thread pools for all threading models.
            int inline_cnt = 0, worker_cnt = 0, tm_num = 0;
            std::vector<std::thread> leaf_client;

            for (std::map<TMNames, struct TMConfig>::iterator it = all_tms.begin(); it != all_tms.end(); ++it)
            {
                TMNames my_tm = it->first;
                for(int k = 0; k < it->second.num_workers; k++)
                {
                    it->second.worker_thread_pool.emplace_back(std::thread(&ServerImpl::Dispatch, this, k, my_tm, tm_num));
                    worker_cnt++;
                }
                tm_num++;
            }
            tm_num = 0;
            for (std::map<TMNames, struct TMConfig>::iterator it = all_tms.begin(); it != all_tms.end(); ++it)
            {
                TMNames my_tm = it->first;
                for(int j = 0; j < it->second.num_inline; j++)
                {
                    it->second.inline_thread_pool.emplace_back(std::thread(&ServerImpl::HandleRpcs, this, j, my_tm, tm_num));
                    inline_cnt++;
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

            for (std::map<TMNames, struct TMConfig>::iterator it = all_tms.begin(); it != all_tms.end(); ++it)
            {
                for(int j = 0; j < it->second.num_inline; j++)
                {
                    it->second.inline_thread_pool[j].join();
                }
                for(int k = 0; k < it->second.num_workers; k++)
                {
                    it->second.worker_thread_pool[k].join();
                }
            }

        }

    private:
        // Class encompasing the state and logic needed to serve a request.
        class CallData {
            public:
                // Take in the "service" instance (in this case representing an asynchronous
                // server) and the completion queue "cq" used for asynchronous communication
                // with the gRPC runtime.
                CallData(MidTierService::AsyncService* service, ServerCompletionQueue* cq)
                    : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
                        // Invoke the serving logic right away.
                        int mid_tier_tid = 0;
                        TMNames my_tm = sip1;
                        Proceed(mid_tier_tid, my_tm);
                    }

                void Proceed(int mid_tier_tid,
                        TMNames my_tm) {
                    if (status_ == CREATE) {
                        // Make this instance progress to the PROCESS state.
                        status_ = PROCESS;

                        // As part of the initial CREATE state, we *request* that the system
                        // start processing SayHello requests. In this request, "this" acts are
                        // the tag uniquely identifying the request (so that different CallData
                        // instances can serve different requests concurrently), in this case
                        // the memory address of this CallData instance.
                        service_->RequestMidTier(&ctx_, &mid_tier_request_, &responder_, cq_, cq_,
                                this);
                    } else if (status_ == PROCESS) {
                        // Spawn a new CallData instance to serve new clients while we process
                        // the one for this CallData. The instance will deallocate itself as
                        // part of its FINISH state.
                        new CallData(service_, cq_);
                        uint64_t request_id = reinterpret_cast<uintptr_t>(this);
                        // The actual processing.
                        uint64_t start = GetTimeInMicro();
                        ProcessRequest(mid_tier_request_, 
                                mid_tier_tid, 
                                request_id, 
                                my_tm, 
                                &mid_tier_reply_);
                        mid_tier_reply_.set_mid_tier_time(GetTimeInMicro() - start);

                        // And we are done! Let the gRPC runtime know we've finished, using the
                        // memory address of this instance as the uniquely identifying tag for
                        // the event.
                        status_ = FINISH;
#ifndef NODEBUG
                        std::cout << "finished\n";
#endif
                        responder_.Finish(mid_tier_reply_, Status::OK, this);

                        if (kill_signal) {
                            sleep(5);
                            CHECK(false, "Exit signal received\n");
                        }
                    } else {
                        //GPR_ASSERT(status_ == FINISH);
                        // Once in the FINISH state, deallocate ourselves (CallData).
                        delete this;
                    }
                }

            private:
                // The means of communication with the gRPC runtime for an asynchronous
                // server.
                MidTierService::AsyncService* service_;
                // The producer-consumer queue where for asynchronous server notifications.
                ServerCompletionQueue* cq_;
                // Context for the rpc, allowing to tweak aspects of it such as the use
                // of compression, authentication, as well as to send metadata back to the
                // client.
                ServerContext ctx_;

                // What we get from the client.
                MidTierRequest mid_tier_request_;
                // What we send back to the client.
                MidTierResponse mid_tier_reply_;

                // The means to get back to the client.
                ServerAsyncResponseWriter<MidTierResponse> responder_;

                // Let's implement a tiny state machine with the following states.
                enum CallStatus { CREATE, PROCESS, FINISH };
                CallStatus status_;  // The current serving state.
        };

        /* Function called by thread that is the worker. Network poller 
           hands requests to this worker thread via a 
           producer-consumer style queue.*/
        void Dispatch(int worker_tid,
                TMNames my_tm,
                int tm_num) {
            /* We need to make sure that we are a thread
               that belongs to the pool of network pollers belonging
               to the current threading model going on.*/
            while(true)
            {
                if (curr_tm->AtomicallyReadTM() != my_tm) {
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

        void InvokeSib(int mid_tier_tid,
                TMNames my_tm,
                void* tag,
                bool* ok)
        {
            //mid_tier_srv_cqs[my_tm][mid_tier_tid]->Next(&tag, ok);
            cq_->Next(&tag, ok);
            LoadMonitor();

            //GPR_ASSERT(ok);
            static_cast<CallData*>(tag)->Proceed(mid_tier_tid, my_tm);
        }

        void InvokeSip(int mid_tier_tid,
                TMNames my_tm,
                void* tag,
                bool* ok)
        {

            //auto r = mid_tier_srv_cqs[my_tm][mid_tier_tid]->AsyncNext(&tag, ok, gpr_time_0(GPR_CLOCK_REALTIME));
            auto r = cq_->AsyncNext(&tag, ok, gpr_time_0(GPR_CLOCK_REALTIME));
#ifndef NODEBUG
            std::cout << "aft sip next\n";
#endif
            if (r == ServerCompletionQueue::GOT_EVENT) {
                LoadMonitor();
                static_cast<CallData*>(tag)->Proceed(mid_tier_tid, my_tm);
            }
            if (r == ServerCompletionQueue::TIMEOUT) 
            {
                return;
            }
        }

        void InvokeSdb(int mid_tier_tid,
                TMNames my_tm,
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

        void InvokeSdp(int mid_tier_tid,
                TMNames my_tm,
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
        void HandleRpcs(int mid_tier_tid, TMNames my_tm, int tm_num) {
            // Spawn a new CallData instance to serve new clients.
            //new CallData(&service_, mid_tier_srv_cqs[my_tm][mid_tier_tid].get());
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
                if (curr_tm->AtomicallyReadTM() != my_tm) {
                    tm_notify[tm_num].Reset();
#ifndef NODEBUG
                    std::cout << "my tm = " << my_tm << " waiting " << std::endl;
#endif
                    tm_notify[tm_num].Wait();
                }
#ifndef NODEBUG
                std::cout << "curr tm = " << curr_tm->AtomicallyReadTM() << " waiting " << std::endl;
#endif

                // Fill your inflection TMs here.

                switch(my_tm) 
                {
                    case sip1:
                        {
                            InvokeSip(mid_tier_tid, my_tm, tag, &ok);
                            break;
                        }
                    case sdp1_20:
                        {
                            InvokeSdp(mid_tier_tid, my_tm, tag, &ok);
                            break;
                        }
                    case sdb1_50:
                        {
                            InvokeSdb(mid_tier_tid, my_tm, tag, &ok);
                            break;
                        }
                    default:
                        CHECK(false, "Panic: Unknown threading model detected\n");

                }
            }
        }

        std::unique_ptr<ServerCompletionQueue> cq_;
        MidTierService::AsyncService service_;
        std::unique_ptr<Server> server_;
};

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

void SwitchThreadingModel(const int load)
{
    /* This is the piecewise linear model. Fill your tail inflection
       definitions here. You can use as is if you are using microtune with
       microsuite.*/
    TMNames prev_tm = curr_tm->AtomicallyReadTM();
    if (load >= 0 && load < 42) {
        if (prev_tm != sip1) {
            std::cout << "sip1" << std::endl;
            curr_tm->AtomicallySetTM(sip1);
            tm_notify[0].Set();
            workers_notify[0].Set();
        } else return;
    } else if (load >= 42 && load < 74) {
        if (prev_tm != sip1) {
            std::cout << "sip1" << std::endl;
            curr_tm->AtomicallySetTM(sip1);
            tm_notify[0].Set();
            workers_notify[0].Set();
        } else return;

    } else if (load >= 74 && load < 138) {
        if (prev_tm != sip1) {
            std::cout << "sip1" << std::endl;
            curr_tm->AtomicallySetTM(sip1);
            tm_notify[0].Set();
            workers_notify[0].Set();
        } else return;

    } else if (load >= 138 && load < 266) {
        if (prev_tm != sip1) {
            std::cout << "sip1" << std::endl;
            curr_tm->AtomicallySetTM(sip1);
            tm_notify[0].Set();
            workers_notify[0].Set();
        } else return;
    } else if (load >= 266 && load < 522) {
        if (prev_tm != sdp1_20) {
            std::cout << "sdp1_20" << std::endl;
            curr_tm->AtomicallySetTM(sdp1_20);
            tm_notify[1].Set();
            workers_notify[1].Set();
        } else return; 
    } else if (load >= 522 && load < 1034) {
        if (prev_tm != sdp1_20) {
            std::cout << "sdp1_20" << std::endl;
            curr_tm->AtomicallySetTM(sdp1_20);
            tm_notify[1].Set();
            workers_notify[1].Set();
        } else return; 
    } else if (load >= 1034 && load < 2058) {
        if (prev_tm != sdp1_20) {
            std::cout << "sdp1_20" << std::endl;
            curr_tm->AtomicallySetTM(sdp1_20);
            tm_notify[1].Set();
            workers_notify[1].Set();
        } else return; 
    } else if (load >= 2058 && load < 4106) {
        if (prev_tm != sdb1_50) {
            std::cout << "sdb1_50" << std::endl;
            curr_tm->AtomicallySetTM(sdb1_50);
            tm_notify[2].Set();
            workers_notify[2].Set();
        } else return; 

    } else if (load >= 4106 && load < 8202) {
        if (prev_tm != sdb1_50) {
            std::cout << "sdb1_50" << std::endl;
            curr_tm->AtomicallySetTM(sdb1_50);
            tm_notify[2].Set();
            workers_notify[2].Set();
        } else return; 
    } else if (load >= 8202) {
        if (prev_tm != sdb1_50) {
            std::cout << "sdb1_50" << std::endl;
            curr_tm->AtomicallySetTM(sdb1_50);
            tm_notify[2].Set();
            workers_notify[2].Set();
        } else return;
    }
}

int main(int argc, char** argv)
{
    if (argc == 6) {
        number_of_leaf_servers = atoi(argv[1]);
        leaf_server_ips_file = argv[2];
        ip = argv[3];
        mid_tier_parallelism = atoi(argv[4]);
        dispatch_parallelism = atoi(argv[5]);
    } else {
        CHECK(false, "<./mid_tier_server> <number of leaf servers> <leaf server ips file> <ip:port number> <mid_tier parallelism> <num of workers>\n");
    }
    // Load leaf server IPs into a string vector
    GetLeafServerIPs(leaf_server_ips_file, &leaf_server_ips);

    /* Before server starts for the 1st time, construct mid_tier for dataset.
       Offline action*/

    /* Launch a background thread that exclusively monitors the load.
       This thread decides what the TM should be, based on the load detected.
       Load is monitored in epochs. This thread monitrs the load for AWAKEEPOCH
       period of time, for every ASLEEPEPOCH.*/

#ifndef NODEBUG
    std::cout << "launched load monitor\n";
#endif

    /* Initialize all threading models i.e
       their name, number of threads associated with each thread pool, etc.*/
    InitializeTMs(TMSIZE, &all_tms);
    tm_notify.resize(TMSIZE);
    workers_notify.resize(TMSIZE);

    // Launch thread pools for all threading models.
    int tid = 0, num_clients = 0;
    std::vector<std::thread> leaf_client;

    for (std::map<TMNames, struct TMConfig>::iterator it = all_tms.begin(); it != all_tms.end(); ++it)
    {
        TMNames my_tm = it->first;

        if ( (my_tm == sip1) ) {
            num_clients = it->second.num_inline;
        } else {
            num_clients = it->second.num_workers;
        }

#ifndef NODEBUG
        std::cout << "num clients = " << num_clients << std::endl;
#endif

        requests_to_leaf_srv_queue[my_tm].resize(num_clients * number_of_leaf_servers);
        resp_recvd_from_leaf_srv[my_tm].resize(num_clients);
        resp_map_mutex[my_tm].resize(num_clients);

#ifndef NODEBUG
        std::cout << "resized parked thread structs\n";
#endif

        for(int i = 0; i < num_clients; i++)
        {
            for(unsigned int j = 0; j < number_of_leaf_servers; j++)
            {
                tid = (i * number_of_leaf_servers) + j;
                std::string ip = leaf_server_ips[j];
                leaf_client.emplace_back(std::thread(LaunchLeafClient, tid, my_tm));
#ifndef NODEBUG
                std::cout << "emplaced leaf clients\n";
#endif
                leaf_connections[my_tm].emplace_back(new LeafServiceClient(grpc::CreateChannel(
                                ip, grpc::InsecureChannelCredentials())));
#ifndef NODEBUG
                std::cout << "emplaced leaf connections\n";
#endif

            }
        }
    }



#ifndef NODEBUG
    std::cout << "Initialized TMs. Starting run\n";
#endif

    ServerImpl server;
    server.Run();


    std::cout << "leaf client size " << leaf_client.size() << std::endl;
    for(int i = 0; i < leaf_client.size(); i++)
    {
        leaf_client[i].join();
    }

    return 0;
}
