#ifndef MMA_H
#define MMA_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

enum {
    MMA_OK = 0,
    MMA_INVALID_ARGUMENT = -1,
    MMA_INVALID_CONFIG = -2,
    MMA_SERVER_ERROR = -3,
    MMA_SERVER_ALREADY_STARTED = -4
};

enum {
    MMA_RESPONSE_OK = 1,
    MMA_RESPONSE_BAD_REQUEST = 2,
    MMA_RESPONSE_UNAUTHORIZED = 3,
    MMA_RESPONSE_FORBIDDEN = 4,
    MMA_RESPONSE_NOT_FOUND = 5,
    MMA_RESPONSE_CONFLICT = 6,
    MMA_RESPONSE_INTERNAL_ERROR = 7,
    MMA_RESPONSE_MESSAGE = 8
};

typedef struct MMA_Server MMA_Server;
typedef struct MMA_Response MMA_Response;

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
} MMA_FramerConfig;

typedef struct {
    const char *bind_ip;
    uint16_t port;
    uint8_t max_batch;
    uint32_t max_in_flight;
    MMA_FramerConfig framer;
} MMA_ServerConfig;

typedef struct {
    const uint8_t *key;
    size_t key_len;
    const uint8_t *value;
    size_t value_len;
} MMA_Option;

typedef struct {
    const uint8_t *route;
    size_t route_len;
    uint8_t opcode;
    uint8_t version;
    uint32_t request_id;
    const uint8_t *payload;
    size_t payload_len;
    const MMA_Option *options;
    size_t options_count;
} MMA_Request;

typedef int32_t (*MMA_RouteCallback)(
    const MMA_Request *request,
    MMA_Response *response,
    void *user_data
);

/*
 * Request and option data are borrowed and valid only during the callback.
 * Response data must be set with the response functions so it is copied safely.
 * A callback must return MMA_OK; any other return value produces an internal
 * error response. Callbacks may execute concurrently on runtime worker threads,
 * so synchronize shared user_data and keep it alive until the server stops.
 * Register routes before starting the server; callbacks should not block.
 */
int32_t mma_server_config_default(MMA_ServerConfig *output);
int32_t mma_server_create(const MMA_ServerConfig *config, MMA_Server **output);
int32_t mma_server_local_port(const MMA_Server *server, uint16_t *port);
int32_t mma_server_route(
    MMA_Server *server,
    const uint8_t *route,
    size_t route_len,
    MMA_RouteCallback callback,
    void *user_data
);
int32_t mma_server_start(MMA_Server *server);

int32_t mma_response_set_status(MMA_Response *response, uint8_t opcode);
int32_t mma_response_set_payload(
    MMA_Response *response,
    const uint8_t *data,
    size_t data_len
);
int32_t mma_response_add_option(
    MMA_Response *response,
    const uint8_t *key,
    size_t key_len,
    const uint8_t *value,
    size_t value_len
);

#ifdef __cplusplus
}
#endif

#endif