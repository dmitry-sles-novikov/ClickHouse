#include <Storages/NATS/NATSProducer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CONNECT_NATS;
}

NATSProducer::NATSProducer(
    const NATSConfiguration & configuration_,
    const String & subject_,
    std::atomic<bool> & shutdown_called_,
    LoggerPtr log_)
    : INATSProducer(configuration_, subject_, shutdown_called_, std::move(log_))
{
}

void NATSProducer::initialize()
{
    if (!getConnection().connect())
        throw Exception(ErrorCodes::CANNOT_CONNECT_NATS, "Cannot connect to NATS {}", getConnection().connectionInfoForLog());
}

natsStatus NATSProducer::publishMessage(const String & message)
{
    return natsConnection_Publish(getNativeConnection(), getSubject().c_str(), message.c_str(), static_cast<int>(message.size()));
}

}
