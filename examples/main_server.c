#include "mma.h"

#include <signal.h>
#include <inttypes.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static volatile sig_atomic_t running = 1;

static void handle_signal(int signal_number)
{
    (void)signal_number;
    running = 0;
}

static int32_t get_route(
    const MMA_Request *request,
    MMA_Response *response,
    void *user_data
)
{
    (void)user_data;

    int32_t status = mma_response_set_status(response, MMA_RESPONSE_OK);
    if (status != MMA_OK) {
        return status;
    }
    return mma_response_set_payload(
        response,
        request->payload,
        request->payload_len
    );
}

static uint32_t wrapping_pow(int32_t base, uint32_t exponent)
{
    uint32_t result = 1;
    uint32_t factor = (uint32_t)base;

    while (exponent > 0) {
        if ((exponent & 1U) != 0) {
            result *= factor;
        }
        exponent >>= 1U;
        if (exponent > 0) {
            factor *= factor;
        }
    }

    return result;
}

static int32_t pow_route(
    const MMA_Request *request,
    MMA_Response *response,
    void *user_data
)
{
    (void)user_data;

    int32_t number = 1;
    if (request->payload_len > 0 && request->payload_len < 64) {
        char buffer[64];
        memcpy(buffer, request->payload, request->payload_len);
        buffer[request->payload_len] = '\0';
        char *end = NULL;
        long parsed = strtol(buffer, &end, 10);
        if (end != buffer && *end == '\0' && parsed >= INT32_MIN && parsed <= INT32_MAX) {
            number = (int32_t)parsed;
        }
    }

    uint32_t exponent = 2;
    for (size_t i = 0; i < request->options_count; ++i) {
        const MMA_Option *option = &request->options[i];
        if (
            option->key_len == 3 &&
            memcmp(option->key, "pow", 3) == 0 &&
            option->value_len > 0 &&
            option->value_len < 32
        ) {
            char buffer[32];
            memcpy(buffer, option->value, option->value_len);
            buffer[option->value_len] = '\0';
            char *end = NULL;
            unsigned long parsed = strtoul(buffer, &end, 10);
            if (
                end != buffer &&
                *end == '\0' &&
                buffer[0] != '-' &&
                parsed <= UINT32_MAX
            ) {
                exponent = (uint32_t)parsed;
            }
            break;
        }
    }

    char payload[64];
    int payload_len = snprintf(
        payload,
        sizeof(payload),
        "%" PRId32 "^%" PRIu32 " = %" PRIu32,
        number,
        exponent,
        wrapping_pow(number, exponent)
    );
    if (payload_len < 0 || (size_t)payload_len >= sizeof(payload)) {
        return MMA_SERVER_ERROR;
    }

    int32_t status = mma_response_set_status(response, MMA_RESPONSE_OK);
    if (status != MMA_OK) {
        return status;
    }
    return mma_response_set_payload(
        response,
        (const uint8_t *)payload,
        (size_t)payload_len
    );
}

int main(void)
{
    MMA_ServerConfig config;
    int32_t result = mma_server_config_default(&config);
    if (result != MMA_OK) {
        fprintf(stderr, "Could not initialize server configuration: %d\n", result);
        return 1;
    }

    config.bind_ip = "0.0.0.0";
    config.port = 8080;
    config.max_batch = 255;
    config.max_in_flight = 1024;
    config.framer.opcode_pos = 1;
    config.framer.version_pos = 4;
    config.framer.max_message_length = 10 * 1000 * 1000;
    config.framer.route_len_pos = 6;
    config.framer.options_count_pos = 7;
    config.framer.route_order = 3;
    config.framer.options_order = 2;
    config.framer.payload_order = 1;
    config.framer.options_key_first = 1;
    config.framer.header_length = 29;

    MMA_Server *server = NULL;
    result = mma_server_create(&config, &server);
    if (result != MMA_OK) {
        fprintf(stderr, "Could not create server: %d\n", result);
        return 1;
    }

    static const uint8_t get[] = "GET";
    result = mma_server_route(server, get, sizeof(get) - 1, get_route, NULL);
    if (result != MMA_OK) {
        fprintf(stderr, "Could not register GET route: %d\n", result);
        return 1;
    }

    static const uint8_t pow[] = "POW";
    result = mma_server_route(server, pow, sizeof(pow) - 1, pow_route, NULL);
    if (result != MMA_OK) {
        fprintf(stderr, "Could not register POW route: %d\n", result);
        return 1;
    }

    uint16_t port = 0;
    result = mma_server_local_port(server, &port);
    if (result != MMA_OK) {
        fprintf(stderr, "Could not get server port: %d\n", result);
        return 1;
    }
    printf("Listening on %s:%u\n", config.bind_ip, (unsigned)port);

    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);

    result = mma_server_start(server);
    if (result != MMA_OK) {
        fprintf(stderr, "Could not start server: %d\n", result);
        return 1;
    }

    while (running) {
        /* Wait for SIGINT or SIGTERM. */
    }

    return 0;
}
