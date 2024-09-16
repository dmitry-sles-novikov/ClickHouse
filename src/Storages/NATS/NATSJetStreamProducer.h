#pragma once

#include <Storages/NATS/INATSProducer.h>

namespace DB
{

class NATSJetStreamProducer : public INATSProducer
{
public:
    NATSJetStreamProducer(
        const NATSConfiguration & configuration_,
        const String & subject_,
        std::atomic<bool> & shutdown_called_,
        LoggerPtr log_);

private:
    void initialize() override;

    virtual natsStatus publishMessage(const String & message) override;

private:
    std::unique_ptr<jsCtx, decltype(&jsCtx_Destroy)> jet_stream_ctx;
    jsOptions jet_stream_options;
};

}
