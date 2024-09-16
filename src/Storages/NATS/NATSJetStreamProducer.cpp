#include <Storages/NATS/NATSJetStreamProducer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CONNECT_NATS;
}

NATSJetStreamProducer::NATSJetStreamProducer(
    const NATSConfiguration & configuration_,
    const String & subject_,
    std::atomic<bool> & shutdown_called_,
    LoggerPtr log_)
    : INATSProducer(configuration_, subject_, shutdown_called_, std::move(log_))
    , jet_stream_ctx(nullptr, &jsCtx_Destroy)
{
}

void NATSJetStreamProducer::initialize()
{
    if (!getConnection().connect())
        throw Exception(ErrorCodes::CANNOT_CONNECT_NATS, "Cannot connect to NATS {}", getConnection().connectionInfoForLog());

    auto er = jsOptions_Init(&jet_stream_options);
    if(er != NATS_OK){
        throw Exception(
            ErrorCodes::CANNOT_CONNECT_NATS,
            "Failed to receive NATS jet stream options for {}. Nats last error: {}",
            getConnection().connectionInfoForLog(), natsStatus_GetText(er));
    }

    jsCtx * new_jet_stream_ctx = nullptr;
    er = natsConnection_JetStream(&new_jet_stream_ctx, getNativeConnection(), &jet_stream_options);
    if(er != NATS_OK){
        throw Exception(
            ErrorCodes::CANNOT_CONNECT_NATS,
            "Failed to create NATS jet stream ctx for {}. Nats last error: {}",
            getConnection().connectionInfoForLog(), natsStatus_GetText(er));
    }
    jet_stream_ctx.reset(new_jet_stream_ctx);
}

natsStatus NATSJetStreamProducer::publishMessage(const String & message)
{
    std::unique_ptr<jsPubAck, decltype(&jsPubAck_Destroy)> ack_response_holder{nullptr, &jsPubAck_Destroy};

    jsPubAck * ack_response = nullptr;
    auto result = js_Publish(&ack_response, jet_stream_ctx.get(), getSubject().c_str(), message.c_str(), static_cast<int>(message.size()), nullptr, nullptr);
    if (result != NATS_OK){
        return result;
    }
    ack_response_holder.reset(ack_response);

    if(ack_response->Duplicate){
        LOG_DEBUG(log, "Duplicate message during publishing to NATS subject. Message: {}.", message);
    }

    return result;
}

}
