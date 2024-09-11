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

INATSConsumer::INATSConsumer(
    std::shared_ptr<NATSConnectionManager> connection_,
    StorageNATS & storage_,
    const std::vector<String> & subjects_,
    const String & subscribe_queue_name,
    LoggerPtr log_,
    uint32_t queue_size_,
    const std::atomic<bool> & stopped_)
    : connection(std::move(connection_))
    , storage(storage_)
    , subjects(subjects_)
    , queue_name(subscribe_queue_name)
    , log(std::move(log_))
    , received(queue_size_)
    , stopped(stopped_)
{
}

void INATSConsumer::unsubscribe()
{
    subscriptions.clear();
}

ReadBufferPtr INATSConsumer::consume()
{
    if (isConsumerStopped() || !received.tryPop(current))
        return nullptr;

    return std::make_shared<ReadBufferFromMemory>(current.message.data(), current.message.size());
}

void INATSConsumer::onMsg(natsConnection *, natsSubscription *, natsMsg * msg_ptr, void * consumer)
{
    std::unique_ptr<natsMsg, decltype(&natsMsg_Destroy)> msg(msg_ptr, &natsMsg_Destroy);

    auto * nats_consumer = static_cast<NATSConsumer *>(consumer);
    const int msg_length = natsMsg_GetDataLength(msg.get());

    if (msg_length)
    {
        String message_received = std::string(natsMsg_GetData(msg.get()), msg_length);
        String subject = natsMsg_GetSubject(msg.get());

        MessageData data = {
            .message = std::move(message_received),
            .subject = std::move(subject),
        };
        if (!nats_consumer->received.push(std::move(data)))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not push to received queue");

        nats_consumer->storage.startStreaming();
    }
}

}
