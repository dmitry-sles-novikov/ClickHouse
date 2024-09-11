#include <atomic>
#include <chrono>
#include <memory>
#include <utility>
#include <Storages/NATS/NATSConsumer.h>
#include <IO/ReadBufferFromMemory.h>
#include "Poco/Timer.h"
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_CONNECT_NATS;
}

NATSConsumer::NATSConsumer(
    std::shared_ptr<NATSConnectionManager> connection_,
    StorageNATS & storage_,
    const std::vector<String> & subjects_,
    const String & subscribe_queue_name,
    LoggerPtr log_,
    uint32_t queue_size_,
    const std::atomic<bool> & stopped_)
    : INATSConsumer(std::move(connection_), storage_, subjects_,  subscribe_queue_name, std::move(log_), queue_size_, stopped_)
{
}

void NATSConsumer::subscribe()
{
    if (isSubscribed())
        return;

    std::vector<NATSSubscriptionPtr> subscriptions;
    subscriptions.reserve(getSubjects().size());

    for (const auto & subject : getSubjects())
    {
        natsSubscription * ns;
        auto status = natsConnection_QueueSubscribe(&ns, getNativeConnection(), subject.c_str(), getQueueName().c_str(), onMsg, static_cast<void *>(this));
        if (status == NATS_OK)
        {
            LOG_DEBUG(getLogger(), "Subscribed to subject {}", subject);
            natsSubscription_SetPendingLimits(ns, -1, -1);
            subscriptions.emplace_back(ns, &natsSubscription_Destroy);
        }
        else
        {
            throw Exception(ErrorCodes::CANNOT_CONNECT_NATS, "Failed to subscribe to subject {}", subject);
        }
    }
    setSubscriptions(std::move(subscriptions));
}

}
