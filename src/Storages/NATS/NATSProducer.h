#pragma once

#include <Storages/NATS/INATSProducer.h>

namespace DB
{

class NATSProducer : public INATSProducer
{
public:
    NATSProducer(
        const NATSConfiguration & configuration_,
        const String & subject_,
        std::atomic<bool> & shutdown_called_,
        LoggerPtr log_);

private:
    void initialize() override;
    natsStatus publishMessage(const String & message) override;
};

}
