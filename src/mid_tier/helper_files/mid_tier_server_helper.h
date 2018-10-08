#include "src/protoc_files/mid_tier.grpc.pb.h"
#include "src/protoc_files/leaf.grpc.pb.h"
#include "thread_safe_queue.cpp"
#include "thread_safe_flag.cpp"
#include "thread_safe_circ_buffer.cpp"
#include "atomics.cpp"

#ifndef __MIDTIER_SERVER_HELPER_H_INCLUDED__
#define __MIDTIER_SERVER_HELPER_H_INCLUDED__

#define CHECK(condition, error_message) if (!condition) {std::cerr << __FILE__ << ": " << __LINE__ << ": " << error_message << "\n"; exit(-1);}


/* Struct contains necessary info for each worker thread. 
   Each worker thread launches a grpc connection to a 
   corresponding leaf server. */

struct LeafSrvTimingInfo {
    uint64_t create_leaf_srv_request_time = 0;
    uint64_t unpack_leaf_srv_req_time = 0;
    uint64_t calculate_leaf_time = 0;
    uint64_t pack_leaf_srv_resp_time = 0;
    uint64_t unpack_leaf_srv_resp_time = 0;
    float cpu_util_leaf_srv = 0.0;
};

struct LeafSrvUtil {
    bool util_present = false;
    uint64_t user_time = 0;
    uint64_t system_time = 0;
    uint64_t io_time = 0;
    uint64_t idle_time = 0;
};

struct ResponseData {
    LeafSrvTimingInfo* leaf_srv_timing_info = new LeafSrvTimingInfo();
    LeafSrvUtil* leaf_srv_util = new LeafSrvUtil();
};

struct ThreadArgs {
    LeafSrvTimingInfo leaf_srv_timing_info;
    LeafSrvUtil leaf_srv_util;
};

struct ResponseMetaData {
    std::vector<ResponseData> response_data;
    uint64_t responses_recvd = 0;
    uint64_t id = 0;
    mid_tier_service::MidTierResponse* mid_tier_reply = new mid_tier_service::MidTierResponse();
};

struct DispatchedData {
    void* tag = NULL;
    int mid_tier_tid = 0;
};

struct ReqToLeafSrv {
    int mid_tier_tid = 0;
    bool util_present = false;
    leaf::LeafRequest request_to_leaf_srv;
    LeafSrvTimingInfo* leaf_srv_timing_info;
    LeafSrvUtil* leaf_srv_util;
    int leaf_server_id = 0;
    uint64_t request_id = 0;
};

struct mutex_wrapper : std::mutex
{
    mutex_wrapper() = default;
    mutex_wrapper(mutex_wrapper const&) noexcept : std::mutex() {}
    bool operator==(mutex_wrapper const&other) noexcept { return this==&other; }
};


struct ThreadSafeQueueReqWrapper : ThreadSafeQueue<ReqToLeafSrv>
{
    ThreadSafeQueueReqWrapper() = default;
    ThreadSafeQueueReqWrapper(ThreadSafeQueueReqWrapper const&) noexcept : ThreadSafeQueue<ReqToLeafSrv>() {}
    bool operator==(ThreadSafeQueueReqWrapper const&other) noexcept {return this==&other; }
};

struct ThreadSafeQueueRespWrapper : ThreadSafeQueue<bool>
{
    ThreadSafeQueueRespWrapper() = default;
    ThreadSafeQueueRespWrapper(ThreadSafeQueueRespWrapper const&) noexcept : ThreadSafeQueue<bool>() {}
    bool operator==(ThreadSafeQueueRespWrapper const&other) noexcept {return this==&other; }
};

struct ThreadSafeFlagWrapper : ThreadSafeFlag<bool>
{
    ThreadSafeFlagWrapper() = default;
    ThreadSafeFlagWrapper(ThreadSafeFlagWrapper const&) noexcept : ThreadSafeFlag<bool>() {}
    bool operator==(ThreadSafeFlagWrapper const&other) noexcept {return this==&other; }
};

struct TMConfig {
    TMConfig(int num_inline, int num_workers, int num_resps, std::vector<std::thread> inline_thread_pool = {}, std::vector<std::thread> leaf_client_thread_pool = {}, std::vector<std::thread> worker_thread_pool = {}, std::vector<std::thread> resp_thread_pool = {})
        : num_inline(num_inline)
        , num_workers(num_workers)
        , num_resps(num_resps)
        , inline_thread_pool(std::move(inline_thread_pool))
        , leaf_client_thread_pool(std::move(leaf_client_thread_pool))
        , worker_thread_pool(std::move(worker_thread_pool))
        , resp_thread_pool(std::move(resp_thread_pool))
        {}
    int num_inline = 1;
    int num_workers = 0;
    int num_resps = 0;
    std::vector<std::thread> inline_thread_pool;
    std::vector<std::thread> leaf_client_thread_pool;
    std::vector<std::thread> worker_thread_pool;
    std::vector<std::thread> resp_thread_pool;
};

// uint64_t refers to the void* to the request's tag - i.e its unique id
typedef std::map<uint64_t, ResponseMetaData> ResponseMap;

/* Bucket server IPs are taken in via a file. This file must be read,
   and the leaf server IPs must be stored in a vector of strings. 
   This is so that different point IDs can be suitably routed to
   different leaf servers (based on the shard).
In: string - leaf server IPs file name
Out: vector of strings - all the leaf server IPs*/
void GetLeafServerIPs(const std::string &leaf_server_ips_file, 
        std::vector<std::string>* leaf_server_ips);

void UnpackMidTierServiceRequest(const mid_tier_service::MidTierRequest &mid_tier_request,
        leaf::LeafRequest* request_to_leaf_srv);

void Merge(const struct ThreadArgs* thread_args,
        const unsigned int number_of_leaf_servers,
        uint64_t* calculate_leaf_time);

void MergeAndPack(const std::vector<ResponseData> &response_data,
        const unsigned int number_of_leaf_servers,
        mid_tier_service::MidTierResponse* mid_tier_reply);

void PackMidTierServiceResponse(
        const struct ThreadArgs* thread_args,
        const unsigned int number_of_leaf_servers,
        mid_tier_service::MidTierResponse* mid_tier_reply);


// Following list of functions apply only to the auto tuner.
void InitializeTMs(const int num_tms, 
        std::map<TMNames, TMConfig>* all_tms);
void InitializeAsyncTMs(const int num_tms,
        std::map<AsyncTMNames, TMConfig>* all_tms);
#endif //__LOADGEN_INDEX_SERVER_HELPER_H_INCLUDED__
