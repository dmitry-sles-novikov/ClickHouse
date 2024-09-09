#include <Storages/NATS/INATSProducer.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <Columns/ColumnString.h>
#include <Common/logger_useful.h>


namespace DB
{

static const auto BATCH = 1000;
static const auto MAX_BUFFERED = 131072;

namespace ErrorCodes
{
    extern const int CANNOT_CONNECT_NATS;
    extern const int LOGICAL_ERROR;
}

INATSProducer::INATSProducer(
    const NATSConfiguration & configuration_,
    const String & subject_,
    std::atomic<bool> & shutdown_called_,
    LoggerPtr log_)
    : AsynchronousMessageProducer(log_)
    , connection(configuration_, log_)
    , subject(subject_)
    , shutdown_called(shutdown_called_)
    , payloads(BATCH)
{
}

void INATSProducer::initialize()
{
    if (!connection.connect())
        throw Exception(ErrorCodes::CANNOT_CONNECT_NATS, "Cannot connect to NATS {}", connection.connectionInfoForLog());
}

void INATSProducer::finishImpl()
{
    connection.disconnect();
}


void INATSProducer::produce(const String & message, size_t, const Columns &, size_t)
{
    if (!payloads.push(message))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not push to payloads queue");
}

void INATSProducer::publish()
{
    uv_thread_t flush_thread;

    uv_thread_create(&flush_thread, publishThreadFunc, static_cast<void *>(this));

    connection.getHandler().startLoop();
    uv_thread_join(&flush_thread);
}

void INATSProducer::publishThreadFunc(void * arg)
{
    INATSProducer * producer = static_cast<INATSProducer *>(arg);
    String payload;

    natsStatus status;
    while (!producer->payloads.empty())
    {
        if (natsConnection_Buffered(producer->connection.getConnection()) > MAX_BUFFERED)
            break;
        bool pop_result = producer->payloads.pop(payload);

        if (!pop_result)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not pop payload");

        status = natsConnection_Publish(producer->connection.getConnection(), producer->subject.c_str(), payload.c_str(), static_cast<int>(payload.size()));

        if (status != NATS_OK)
        {
            LOG_DEBUG(producer->log, "Something went wrong during publishing to NATS subject. Nats status text: {}. Last error message: {}",
                      natsStatus_GetText(status), nats_GetLastError(nullptr));
            if (!producer->payloads.push(std::move(payload)))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not push to payloads queue");
            break;
        }
    }

    nats_ReleaseThreadMemory();
}

void INATSProducer::stopProducingTask()
{
    payloads.finish();
}

void INATSProducer::startProducingTaskLoop()
{
    try
    {
        while ((!payloads.isFinishedAndEmpty() || natsConnection_Buffered(connection.getConnection()) != 0) && !shutdown_called.load())
        {
            publish();

            if (!connection.isConnected())
                connection.reconnect();

            iterateEventLoop();
        }
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }

    LOG_DEBUG(log, "Producer on subject {} completed", subject);
}


void INATSProducer::iterateEventLoop()
{
    connection.getHandler().iterateLoop();
}

}
