#pragma once

#include <nats.h>
#include <Core/Names.h>
#include <Storages/NATS/NATSConnection.h>
#include <Storages/NATS/StorageNATS.h>
#include <Storages/NATS/INATSConsumer.h>

namespace Poco
{
class Logger;
}

namespace DB
{

class NATSConsumer : public INATSConsumer
{
public:
    NATSConsumer(
        std::shared_ptr<NATSConnectionManager> connection_,
        StorageNATS & storage_,
        const std::vector<String> & subjects_,
        const String & subscribe_queue_name,
        LoggerPtr log_,
        uint32_t queue_size_,
        const std::atomic<bool> & stopped_);

    void subscribe() override;

    size_t subjectsCount() { return getSubjects().size(); }

private:
    String channel_id;
};

}
