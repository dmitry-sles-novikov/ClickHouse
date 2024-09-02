#pragma once

#include <IO/ReadBuffer.h>
#include <base/types.h>
#include <Common/ConcurrentBoundedQueue.h>

namespace Poco
{
class Logger;
}

namespace DB
{

class INATSConsumer
{
public:
    struct MessageData
    {
        String message;
        String subject;
    };

public:
    INATSConsumer(
        std::shared_ptr<NATSConnectionManager> connection_,
        StorageNATS & storage_,
        const std::vector<String> & subjects_,
        LoggerPtr log_,
        uint32_t queue_size_,
        const std::atomic<bool> & stopped_);
    virtual ~INATSConsumer() = default;

    virtual void subscribe() = 0;
    void unsubscribe();

    /// Return read buffer containing next available message
    /// or nullptr if there are no messages to process.
    ReadBufferPtr consume();

    bool isConsumerStopped() { return stopped; }

    bool queueEmpty() { return received.empty(); }
    size_t queueSize() { return received.size(); }

    auto getSubject() const { return current.subject; }
    const String & getCurrentMessage() const { return current.message; }

protected:
    const std::shared_ptr<NATSConnectionManager> & getConnection(){return connection;}
    natsConnection * getNativeConnection(){return connection->getConnection();}

    const std::vector<String> & getSubjects() const{return subjects;}
    const LoggerPtr & getLogger() const{return log;}

    static void onMsg(natsConnection * nc, natsSubscription * sub, natsMsg * msg, void * consumer);

    bool isSubscribed() const{return !subscriptions.empty();}
    void setSubscriptions(std::vector<NATSSubscriptionPtr> && subscriptions_){subscriptions = std::move(subscriptions_);}

private:
    std::shared_ptr<NATSConnectionManager> connection;
    StorageNATS & storage;
    const std::vector<String> subjects;
    std::vector<NATSSubscriptionPtr> subscriptions;
    LoggerPtr log;

    ConcurrentBoundedQueue<MessageData> received;
    MessageData current;

    const std::atomic<bool> & stopped;
};

}
