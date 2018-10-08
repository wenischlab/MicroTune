#include <fstream>
#include <iterator>
#include <sstream>
#include <string>
#include <sys/time.h>
#include <unordered_map>
#include "mid_tier_server_helper.h"

void GetLeafServerIPs(const std::string &leaf_server_ips_file,
        std::vector<std::string>* leaf_server_ips)
{
    std::ifstream file(leaf_server_ips_file);
    CHECK((file.good()), "ERROR: File containing leaf server IPs must exists\n");
    std::string line = "";
    for(int i = 0; std::getline(file, line); i++)
    {
        std::istringstream buffer(line);
        std::istream_iterator<std::string> begin(buffer), end;
        std::vector<std::string> tokens(begin, end);
        /* Tokens must contain only one IP -
           Each IP address must be on a different line.*/
        CHECK((tokens.size() == 1), "ERROR: File must contain only one IP address per line\n");
        leaf_server_ips->push_back(tokens[0]);
    }
}

void Merge(const struct ThreadArgs* thread_args,
        const unsigned int number_of_leaf_servers,
        uint64_t* calculate_leaf_time);
{
    for(unsigned int j = 0; j < number_of_leaf_servers; j++)
    {
        (*calculate_leaf_time) += thread_args[j].leaf_srv_timing_info.calculate_leaf_time;
    }
    (*calculate_leaf_time) = (*calculate_leaf_time)/number_of_leaf_servers;
}

void MergeAndPack(const std::vector<ResponseData> &response_data,
        const unsigned int number_of_leaf_servers,
        mid_tier_service::MidTierResponse* mid_tier_reply)
{
    uint64_t create_leaf_srv_req_time = 0, unpack_leaf_srv_resp_time = 0, unpack_leaf_srv_req_time = 0, calculate_leaf_time = 0, pack_leaf_srv_resp_time = 0;
    for(unsigned int j = 0; j < number_of_leaf_servers; j++)
    {
        calculate_leaf_time += response_data[j].leaf_srv_timing_info->calculate_leaf_time;

        mid_tier_service::Util* leaf_srv_util = mid_tier_reply->mutable_util_response()->add_leaf_srv_util();
        leaf_srv_util->set_user_time(response_data[j].leaf_srv_util->user_time);
        leaf_srv_util->set_system_time(response_data[j].leaf_srv_util->system_time);
        leaf_srv_util->set_io_time(response_data[j].leaf_srv_util->io_time);
        leaf_srv_util->set_idle_time(response_data[j].leaf_srv_util->idle_time);

    }
    calculate_leaf_time = calculate_leaf_time/number_of_leaf_servers;

    mid_tier_reply->set_calculate_leaf_time(calculate_leaf_time);
    mid_tier_reply->set_number_of_leaf_servers(number_of_leaf_servers);
}

void PackMidTierServiceResponse(
        const struct ThreadArgs* thread_args,
        const unsigned int number_of_leaf_servers,
        mid_tier_service::MidTierResponse* mid_tier_reply)
{
    for(unsigned int i = 0; i < number_of_leaf_servers; i++)
    {
        mid_tier_service::Util* leaf_srv_util = mid_tier_reply->mutable_util_response()->add_leaf_srv_util();
        leaf_srv_util->set_user_time(thread_args[i].leaf_srv_util.user_time);
        leaf_srv_util->set_system_time(thread_args[i].leaf_srv_util.system_time);
        leaf_srv_util->set_io_time(thread_args[i].leaf_srv_util.io_time);
        leaf_srv_util->set_idle_time(thread_args[i].leaf_srv_util.idle_time);
    }
}

void InitializeTMs(const int num_tms,
        std::map<TMNames, TMConfig>* all_tms)
{
    for(int i = 0; i < num_tms; i++)
    {
        switch(i)
        {
            case 0: 
                {
                    TMNames tm_name = sip1;
#ifndef NODEBUG
                    std::cout << "before assigning\n";
#endif
                    all_tms->insert(std::pair<TMNames, TMConfig>(tm_name, TMConfig(1, 0, 0)));
#ifndef NODEBUG
                    std::cout << "after assigning\n";
#endif
                    break;
                }
            case 1:
                {
                    TMNames tm_name = sdp1_20;
#ifndef NODEBUG
                    std::cout << "before assigning\n";
#endif
                    all_tms->insert(std::pair<TMNames, TMConfig>(tm_name, TMConfig(1, 20, 0)));
#ifndef NODEBUG
                    std::cout << "after assigning\n";
#endif

                    break;
                }
            case 2:
                {
                    TMNames tm_name = sdb1_50;
#ifndef NODEBUG
                    std::cout << "before assigning\n";
#endif
                    all_tms->insert(std::pair<TMNames, TMConfig>(tm_name, TMConfig(1, 50, 0)));
#ifndef NODEBUG
                    std::cout << "after assigning\n";
#endif
                    break;
                }
#if 0
            case 3:
                {
                    TMNames tm_name = sdb30_50;
#ifndef NODEBUG
                    std::cout << "before assigning\n";
#endif
                    all_tms->insert(std::pair<TMNames, TMConfig>(tm_name, TMConfig(30, 50, 0)));
#ifndef NODEBUG
                    std::cout << "after assigning\n";
#endif
                    break;
                }
            case 4:
                {
                    TMNames tm_name = sdb30_10;
#ifndef NODEBUG
                    std::cout << "before assigning\n";
#endif
                    all_tms->insert(std::pair<TMNames, TMConfig>(tm_name, TMConfig(30, 10, 0)));
#ifndef NODEBUG
                    std::cout << "after assigning\n";
#endif
                    break;
                }

            case 5:
                {
                    TMNames tm_name = sdb40_30;
#ifndef NODEBUG
                    std::cout << "before assigning\n";
#endif
                    all_tms->insert(std::pair<TMNames, TMConfig>(tm_name, TMConfig(40, 30, 0)));
#ifndef NODEBUG
                    std::cout << "after assigning\n";
#endif
                    break;
                }

            case 6:
                {
                    TMNames tm_name = sdb50_20;
#ifndef NODEBUG
                    std::cout << "before assigning\n";
#endif
                    all_tms->insert(std::pair<TMNames, TMConfig>(tm_name, TMConfig(50, 20, 0)));
#ifndef NODEBUG
                    std::cout << "after assigning\n";
#endif
                    break;
                }

            case 7:
                {
                    TMNames tm_name = sdp1_50;
#ifndef NODEBUG
                    std::cout << "before assigning\n";
#endif
                    all_tms->insert(std::pair<TMNames, TMConfig>(tm_name, TMConfig(1, 50, 0)));
#ifndef NODEBUG
                    std::cout << "after assigning\n";
#endif
                    break;
                }
#endif
        }

    }
}

void InitializeAsyncTMs(const int num_tms,
        std::map<AsyncTMNames, TMConfig>* all_tms)
{
    for(int i = 0; i < num_tms; i++)
    {
        switch(i)
        {
            case 0:
                {
                    AsyncTMNames tm_name = aip1_0_1;
#ifndef NODEBUG
                    std::cout << "before assigning\n";
#endif
                    all_tms->insert(std::pair<AsyncTMNames, TMConfig>(tm_name, TMConfig(1, 0, 1)));
#ifndef NODEBUG
                    std::cout << "after assigning\n";
#endif
                    break;
                }
            case 1:
                {
                    AsyncTMNames tm_name = adp1_4_1;
#ifndef NODEBUG
                    std::cout << "before assigning\n";
#endif
                    all_tms->insert(std::pair<AsyncTMNames, TMConfig>(tm_name, TMConfig(1, 4, 1)));
#ifndef NODEBUG
                    std::cout << "after assigning\n";
#endif
                    break;
                }
            case 2:
                {
                    AsyncTMNames tm_name = adb1_4_4;
#ifndef NODEBUG
                    std::cout << "before assigning\n";
#endif
                    all_tms->insert(std::pair<AsyncTMNames, TMConfig>(tm_name, TMConfig(1, 4, 4)));
#ifndef NODEBUG
                    std::cout << "after assigning\n";
#endif
                    break;
                }
        }

    }
}


