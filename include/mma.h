
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
    MMA_RESPONSE_MESSAGE = 8,
    MMA_RESPONSE_SHUTDOWN = 9,
    MMA_RESPONSE_CHANNEL_CONNECTED = 10,
    MMA_RESPONSE_TIMEOUT = 11
};

typedef struct MMA_Server MMA_Server;
typedef struct MMA_ServerHandle MMA_ServerHandle;
typedef struct MMA_Response {
    const void *inner;
} MMA_Response;

typedef struct {
    uint8_t options_len_order;
    uint8_t payload_len_order;
    uint8_t req_id_order;
} MMA_BigBytesConfig;

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
    MMA_BigBytesConfig big_bytes_config;
} MMA_FramerConfig;

typedef struct {
    const char *bind_ip;
    uint16_t port;
    uint32_t max_batch;
    uint32_t max_in_flight;
    MMA_FramerConfig framer;
    uint32_t max_connections;
    /* Timeouts are expressed in milliseconds. */
    uint64_t drain_timeout_ms;
    uint64_t shutdown_grace_ms;
    uint64_t request_timeout_ms;
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

typedef MMA_Response (*MMA_RouteCallback)(
    MMA_Request request,
    MMA_Response response,
    const void *user_data
);

typedef int32_t (*MMA_ChannelHandlerCallback)(
    const MMA_Request *request,
    uint32_t connection_id,
    const void *user_data
);

/*
 * Request and option data are borrowed and valid only during the callback.
 * Route callbacks receive a response value and return the response value after
 * applying zero or more transformations with mma_response_* functions.
 * Callbacks may execute concurrently on runtime worker threads,
 * so synchronize shared user_data and keep it alive until the server stops.
 * Register routes before starting the server; callbacks should not block.
 */
int32_t mma_server_config_default(MMA_ServerConfig *output);
int32_t mma_server_create(const MMA_ServerConfig *config, MMA_Server **output);
int32_t mma_server_local_port(const MMA_Server *server, uint16_t *port);
int32_t mma_server_register_route(
    MMA_Server *server,
    const uint8_t *route,
    size_t route_len,
    MMA_RouteCallback callback,
    const void *user_data
);
int32_t mma_server_register_channel_handler(
    MMA_Server *server,
    const uint8_t *route,
    size_t route_len,
    MMA_ChannelHandlerCallback callback,
    const void *user_data
);
/* Backward-compatible alias for mma_server_register_route. */
int32_t mma_server_route(
    MMA_Server *server,
    const uint8_t *route,
    size_t route_len,
    MMA_RouteCallback callback,
    const void *user_data
);
int32_t mma_server_start(MMA_Server *server);
int32_t mma_server_stop(MMA_Server *server);
void mma_server_destroy(MMA_Server *server);

MMA_Response mma_response_set_status(MMA_Response response, uint8_t opcode);
MMA_Response mma_response_set_payload(
    MMA_Response response,
    const uint8_t *data,
    size_t data_len
);
MMA_Response mma_response_add_option(
    MMA_Response response,
    const uint8_t *key,
    size_t key_len,
    const uint8_t *value,
    size_t value_len
);

int32_t mma_server_get_handle(
    const MMA_Server *server,
    MMA_ServerHandle **output
);

void mma_server_handle_destroy(
    MMA_ServerHandle *handle
);

int32_t mma_server_handle_send_to_channel(
    const MMA_ServerHandle *handle,
    uint32_t conn_id,
    MMA_Response message
);

int32_t mma_server_handle_change_config(
    const MMA_ServerHandle *handle,
    const MMA_ServerConfig *config
);

MMA_Response mma_response_new(uint32_t request_id);
void mma_response_destroy(MMA_Response response);

#ifdef __cplusplus
}
#endif

#endif
