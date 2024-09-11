#include <atomic>
#include <chrono>
#include <memory>
#include <utility>
#include <Storages/NATS/NATSJetStreamConsumer.h>
#include <IO/ReadBufferFromMemory.h>
#include "Poco/Timer.h"
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CONNECT_NATS;
}

NATSJetStreamConsumer::NATSJetStreamConsumer(
    std::shared_ptr<NATSConnectionManager> connection_,
    StorageNATS & storage_,
    String stream_name_,
    std::optional<String> consumer_name_,
    const std::vector<String> & subjects_,
    const String & subscribe_queue_name,
    LoggerPtr log_,
    uint32_t queue_size_,
    const std::atomic<bool> & stopped_)
    : INATSConsumer(std::move(connection_), storage_, subjects_, subscribe_queue_name, std::move(log_), queue_size_, stopped_)
    , stream_name(std::move(stream_name_))
    , consumer_name(std::move(consumer_name_))
    , jet_stream_ctx(nullptr, &jsCtx_Destroy)
{
}

void NATSJetStreamConsumer::subscribe()
{
    if (isSubscribed())
        return;

    auto er = jsOptions_Init(&jet_stream_options);
    if(er != NATS_OK){
        throw Exception(
            ErrorCodes::CANNOT_CONNECT_NATS,
            "Failed to receive NATS jet stream options for {}. Nats last error: {}",
            getConnection()->connectionInfoForLog(), natsStatus_GetText(er));
    }

    jsCtx * new_jet_stream_ctx = nullptr;
    er = natsConnection_JetStream(&new_jet_stream_ctx, getNativeConnection(), &jet_stream_options);
    if(er != NATS_OK){
        throw Exception(
            ErrorCodes::CANNOT_CONNECT_NATS,
            "Failed to create NATS jet stream ctx for {}. Nats last error: {}",
            getConnection()->connectionInfoForLog(), natsStatus_GetText(er));
    }
    jet_stream_ctx.reset(new_jet_stream_ctx);

    er = jsSubOptions_Init(&subscribe_options);
    if(er != NATS_OK){
        throw Exception(
            ErrorCodes::CANNOT_CONNECT_NATS,
            "Failed to receive NATS jet stream subscribe options for {}. Error: {}",
            getConnection()->connectionInfoForLog(), natsStatus_GetText(er));
    }

    subscribe_options.Stream = stream_name.c_str();
    if(consumer_name){
        subscribe_options.Consumer = consumer_name->c_str();
    }
    subscribe_options.Queue = getQueueName().c_str();

    std::vector<NATSSubscriptionPtr> subscriptions;
    subscriptions.reserve(getSubjects().size());

    for (const auto & subject : getSubjects())
    {
        natsSubscription * subscription;
        er = js_Subscribe(&subscription, jet_stream_ctx.get(), subject.c_str(), onMsg, static_cast<void *>(this), &jet_stream_options, &subscribe_options, nullptr);
        if (er != NATS_OK){
            throw Exception(ErrorCodes::CANNOT_CONNECT_NATS, "Failed to subscribe to subject {}. Error: {}", subject, natsStatus_GetText(er));
        }
        subscriptions.emplace_back(subscription, &natsSubscription_Destroy);

        LOG_DEBUG(getLogger(), "Subscribed to subject {}", subject);
        natsSubscription_SetPendingLimits(subscription, -1, -1);
    }
    setSubscriptions(std::move(subscriptions));
}

}
