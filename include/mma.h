#ifndef MMA_H
#define MMA_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MMA_STATUS_OK 0
#define MMA_STATUS_INCOMPLETE 1
#define MMA_STATUS_INVALID_ARGUMENT -1
#define MMA_STATUS_INVALID_CONFIG -2
#define MMA_STATUS_ENCODE_ERROR -3
#define MMA_STATUS_DECODE_ERROR -4
#define MMA_STATUS_SERVER_ERROR -5
#define MMA_STATUS_SERVER_RUNNING -6
#define MMA_STATUS_CALLBACK_ERROR -7

typedef struct MMAFramer MMAFramer;
typedef struct MMARequest MMARequest;
typedef struct MMAServer MMAServer;
typedef struct MMAResponseBuilder MMAResponseBuilder;

typedef struct {
    uint32_t max_message_length;
    uint8_t opcode_pos;
    uint8_t version_pos;
    uint8_t route_len_pos;
    uint8_t route_order;
    uint8_t payload_order;
    uint8_t options_order;
    uint8_t options_count_pos;
    uint8_t options_key_first;
    uint8_t header_length;
} MMAFramerConfig;

typedef struct {
    const uint8_t *key;
    size_t key_len;
    const uint8_t *value;
    size_t value_len;
} MMAOption;

typedef struct {
    uint8_t opcode;
    uint8_t version;
    uint32_t req_id;
    const uint8_t *payload;
    size_t payload_len;
    const MMAOption *options;
    size_t options_count;
} MMAResponse;

typedef struct {
    uint8_t *data;
    size_t len;
} MMABytes;

typedef struct {
    const char *bind_ip;
    uint16_t port;
    uint8_t max_batch;
    MMAFramerConfig framer;
} MMAServerConfig;

typedef int32_t (*MMARouteCallback)(
    const MMARequest *request,
    MMAResponseBuilder *response,
    void *user_data
);

int32_t mma_framer_config_default(MMAFramerConfig *output);
int32_t mma_framer_create(const MMAFramerConfig *config, MMAFramer **output);
void mma_framer_destroy(MMAFramer *framer);

int32_t mma_response_encode(
    MMAFramer *framer,
    const MMAResponse *response,
    MMABytes *output
);
void mma_bytes_free(MMABytes bytes);

int32_t mma_framer_decode(
    MMAFramer *framer,
    const uint8_t *data,
    size_t data_len,
    MMARequest **output
);
void mma_request_destroy(MMARequest *request);
int32_t mma_request_payload(
    const MMARequest *request,
    const uint8_t **data,
    size_t *len
);
int32_t mma_request_route(
    const MMARequest *request,
    const uint8_t **data,
    size_t *len
);
int32_t mma_request_metadata(
    const MMARequest *request,
    uint8_t *opcode,
    uint8_t *version,
    uint32_t *req_id
);
int32_t mma_request_options_count(const MMARequest *request, size_t *count);
int32_t mma_request_option(
    const MMARequest *request,
    size_t index,
    const uint8_t **key,
    size_t *key_len,
    const uint8_t **value,
    size_t *value_len
);

int32_t mma_server_config_default(MMAServerConfig *output);
int32_t mma_server_create(const MMAServerConfig *config, MMAServer **output);
int32_t mma_server_register_route(
    MMAServer *server,
    const uint8_t *route,
    size_t route_len,
    MMARouteCallback callback,
    void *user_data
);
int32_t mma_server_start(MMAServer *server);
int32_t mma_server_bound_port(const MMAServer *server, uint16_t *port);
int32_t mma_server_stop(MMAServer *server);
void mma_server_destroy(MMAServer *server);

int32_t mma_response_builder_set_status(
    MMAResponseBuilder *builder,
    uint8_t opcode,
    uint8_t version
);
int32_t mma_response_builder_set_payload(
    MMAResponseBuilder *builder,
    const uint8_t *data,
    size_t data_len
);
int32_t mma_response_builder_add_option(
    MMAResponseBuilder *builder,
    const uint8_t *key,
    size_t key_len,
    const uint8_t *value,
    size_t value_len
);

const char *mma_status_message(int32_t status);

#ifdef __cplusplus
}
#endif

#endif
