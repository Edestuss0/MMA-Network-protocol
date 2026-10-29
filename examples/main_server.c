#include "mma.h"

#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Тайм-ауты задаются в миллисекундах. */
#define MMA_DRAIN_TIMEOUT_MS 2000ULL
#define MMA_SHUTDOWN_GRACE_MS 10000ULL
#define MMA_REQUEST_TIMEOUT_MS 10000ULL

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t ready;
    MMA_ServerHandle *handle;
    bool initialized;
} ChannelContext;

static ChannelContext channel_context = {
    .mutex = PTHREAD_MUTEX_INITIALIZER,
    .ready = PTHREAD_COND_INITIALIZER,
    .handle = NULL,
    .initialized = false
};

static void publish_channel_handle(MMA_ServerHandle *handle)
{
    pthread_mutex_lock(&channel_context.mutex);
    channel_context.handle = handle;
    channel_context.initialized = true;
    pthread_cond_broadcast(&channel_context.ready);
    pthread_mutex_unlock(&channel_context.mutex);
}

static MMA_Response get_route(
    MMA_Request request,
    MMA_Response response,
    const void *user_data
)
{
    (void)user_data;

    response = mma_response_set_status(response, MMA_RESPONSE_OK);
    if (response.inner == NULL) {
        return response;
    }

    return mma_response_set_payload(
        response,
        request.payload,
        request.payload_len
    );
}

/* Возведение в степень по модулю 2^32. */
static uint32_t wrapping_pow(int32_t base, uint32_t exponent)
{
    uint32_t result = 1;
    uint32_t factor = (uint32_t)base;

    while (exponent > 0) {
        if ((exponent & 1U) != 0U) {
            result *= factor;
        }

        exponent >>= 1U;

        if (exponent > 0) {
            factor *= factor;
        }
    }

    return result;
}

static MMA_Response pow_route(
    MMA_Request request,
    MMA_Response response,
    const void *user_data
)
{
    (void)user_data;

    int32_t number = 1;

    if (request.payload != NULL &&
        request.payload_len > 0 &&
        request.payload_len < 64) {
        char buffer[64];

        memcpy(buffer, request.payload, request.payload_len);
        buffer[request.payload_len] = '\0';

        char *end = NULL;
        long parsed = strtol(buffer, &end, 10);

        if (end != buffer &&
            *end == '\0' &&
            parsed >= INT32_MIN &&
            parsed <= INT32_MAX) {
            number = (int32_t)parsed;
        }
    }

    uint32_t exponent = 2;

    for (size_t i = 0;
         request.options != NULL && i < request.options_count;
         ++i) {
        const MMA_Option *option = &request.options[i];

        if (option->key == NULL || option->value == NULL) {
            continue;
        }

        if (option->key_len == 3 &&
            memcmp(option->key, "pow", 3) == 0 &&
            option->value_len > 0 &&
            option->value_len < 32) {
            char buffer[32];

            memcpy(buffer, option->value, option->value_len);
            buffer[option->value_len] = '\0';

            char *end = NULL;
            unsigned long parsed = strtoul(buffer, &end, 10);

            if (end != buffer &&
                *end == '\0' &&
                buffer[0] != '-' &&
                parsed <= UINT32_MAX) {
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

    if (payload_len < 0 ||
        (size_t)payload_len >= sizeof(payload)) {
        return response;
    }

    response = mma_response_set_status(response, MMA_RESPONSE_OK);
    if (response.inner == NULL) {
        return response;
    }

    return mma_response_set_payload(
        response,
        (const uint8_t *)payload,
        (size_t)payload_len
    );
}

/*
 * Обработчик ChannelMessage для маршрута SEND.
 * Payload входящего запроса рассылается всем подписчикам
 * посредством conn_id == 0.
 */
static int32_t send_channel(
    const MMA_Request *request,
    uint32_t connection_id,
    const void *user_data
)
{
    (void)connection_id;

    if (request == NULL || user_data == NULL) {
        return MMA_INVALID_ARGUMENT;
    }

    if (request->payload_len > 0 && request->payload == NULL) {
        return MMA_INVALID_ARGUMENT;
    }

    ChannelContext *context = (ChannelContext *)user_data;

    if (pthread_mutex_lock(&context->mutex) != 0) {
        return MMA_SERVER_ERROR;
    }

    while (!context->initialized) {
        int wait_result =
            pthread_cond_wait(&context->ready, &context->mutex);

        if (wait_result != 0) {
            pthread_mutex_unlock(&context->mutex);
            return MMA_SERVER_ERROR;
        }
    }

    MMA_ServerHandle *handle = context->handle;
    pthread_mutex_unlock(&context->mutex);

    if (handle == NULL) {
        return MMA_SERVER_ERROR;
    }

    MMA_Response message = mma_response_new(request->request_id);

    if (message.inner == NULL) {
        return MMA_SERVER_ERROR;
    }

    MMA_Response updated = mma_response_set_status(
        message,
        MMA_RESPONSE_MESSAGE
    );

    if (updated.inner == NULL) {
        mma_response_destroy(message);
        return MMA_SERVER_ERROR;
    }

    message = updated;

    updated = mma_response_set_payload(
        message,
        request->payload,
        request->payload_len
    );

    if (updated.inner == NULL) {
        mma_response_destroy(message);
        return MMA_SERVER_ERROR;
    }

    message = updated;

    int32_t result = mma_server_handle_send_to_channel(
        handle,
        0,
        message
    );

    /* send_to_channel должен клонировать Response. */
    mma_response_destroy(message);

    return result;
}

static int32_t register_routes(MMA_Server *server)
{
    static const uint8_t get_name[] = "GET";
    static const uint8_t pow_name[] = "POW";
    static const uint8_t send_name[] = "SEND";

    int32_t result = mma_server_register_route(
        server,
        get_name,
        sizeof(get_name) - 1,
        get_route,
        NULL
    );

    if (result != MMA_OK) {
        fprintf(stderr, "Could not register GET route: %d\n", result);
        return result;
    }

    result = mma_server_register_route(
        server,
        pow_name,
        sizeof(pow_name) - 1,
        pow_route,
        NULL
    );

    if (result != MMA_OK) {
        fprintf(stderr, "Could not register POW route: %d\n", result);
        return result;
    }

    result = mma_server_register_channel_handler(
        server,
        send_name,
        sizeof(send_name) - 1,
        send_channel,
        &channel_context
    );

    if (result != MMA_OK) {
        fprintf(stderr, "Could not register SEND handler: %d\n", result);
        return result;
    }

    return MMA_OK;
}

int main(void)
{
    sigset_t signal_set;

    if (sigemptyset(&signal_set) != 0 ||
        sigaddset(&signal_set, SIGINT) != 0 ||
        sigaddset(&signal_set, SIGTERM) != 0) {
        perror("Could not configure signal set");
        return EXIT_FAILURE;
    }

    int mask_result = pthread_sigmask(SIG_BLOCK, &signal_set, NULL);

    if (mask_result != 0) {
        fprintf(
            stderr,
            "Could not block termination signals: %s\n",
            strerror(mask_result)
        );
        return EXIT_FAILURE;
    }

    MMA_ServerConfig config;

    int32_t result = mma_server_config_default(&config);

    if (result != MMA_OK) {
        fprintf(
            stderr,
            "Could not initialize server configuration: %d\n",
            result
        );
        return EXIT_FAILURE;
    }

    config.bind_ip = "0.0.0.0";
    config.port = 8080;
    config.max_batch = 65535;
    config.max_in_flight = 65535;
    config.max_connections = 128000;

    config.drain_timeout_ms = MMA_DRAIN_TIMEOUT_MS;
    config.shutdown_grace_ms = MMA_SHUTDOWN_GRACE_MS;
    config.request_timeout_ms = MMA_REQUEST_TIMEOUT_MS;

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

    if (result != MMA_OK || server == NULL) {
        fprintf(stderr, "Could not create server: %d\n", result);
        return EXIT_FAILURE;
    }

    result = register_routes(server);

    if (result != MMA_OK) {
        mma_server_destroy(server);
        return EXIT_FAILURE;
    }

    result = mma_server_start(server);

    if (result != MMA_OK) {
        fprintf(stderr, "Could not start server: %d\n", result);

        publish_channel_handle(NULL);
        mma_server_destroy(server);

        return EXIT_FAILURE;
    }

    MMA_ServerHandle *handle = NULL;

    result = mma_server_get_handle(server, &handle);

    /*
     * Публикуем результат даже при ошибке, чтобы callback,
     * ожидающий condition variable, не завис навсегда.
     */
    publish_channel_handle(
        result == MMA_OK ? handle : NULL
    );

    if (result != MMA_OK || handle == NULL) {
        fprintf(stderr, "Could not get server handle: %d\n", result);

        mma_server_stop(server);
        mma_server_destroy(server);

        return EXIT_FAILURE;
    }

    uint16_t port = 0;

    result = mma_server_local_port(server, &port);

    if (result != MMA_OK) {
        fprintf(stderr, "Could not get server port: %d\n", result);

        mma_server_stop(server);
        publish_channel_handle(NULL);
        mma_server_handle_destroy(handle);
        mma_server_destroy(server);

        return EXIT_FAILURE;
    }

    printf("Listening on %s:%u\n", config.bind_ip, (unsigned)port);
    printf("MMA channel broadcast is ready.\n");
    fflush(stdout);

    int received_signal = 0;
    int wait_result = sigwait(&signal_set, &received_signal);

    if (wait_result != 0) {
        fprintf(stderr, "sigwait failed: %s\n", strerror(wait_result));
    } else {
        printf("Received signal %d; shutting down...\n", received_signal);
    }

    result = mma_server_stop(server);

    if (result != MMA_OK) {
        fprintf(stderr, "Could not stop server cleanly: %d\n", result);
    }

    /*
     * После остановки потока сервера callback больше не должен
     * обращаться к MMA_ServerHandle.
     */
    publish_channel_handle(NULL);

    mma_server_handle_destroy(handle);
    mma_server_destroy(server);

    return (wait_result == 0 && result == MMA_OK)
        ? EXIT_SUCCESS
        : EXIT_FAILURE;
}