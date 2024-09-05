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

class NATSJetStreamConsumer : public INATSConsumer
{
public:
    NATSJetStreamConsumer(
        std::shared_ptr<NATSConnectionManager> connection_,
        StorageNATS & storage_,
        String stream_name_,
        std::optional<String> consumer_name_,
        const std::vector<String> & subjects_,
        LoggerPtr log_,
        uint32_t queue_size_,
        const std::atomic<bool> & stopped_);

    void subscribe() override;

private:
    const String stream_name;
    const std::optional<String> consumer_name;

    std::unique_ptr<jsCtx, decltype(&jsCtx_Destroy)> jet_stream_ctx;
    jsOptions jet_stream_options;
    jsSubOptions subscribe_options;
};

}
