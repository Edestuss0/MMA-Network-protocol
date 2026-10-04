#include "mma.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#define MMA_OPCODE_OK 1
#define MMA_VERSION 1

static void print_status(const char *operation, int32_t status) {
    if (status != MMA_STATUS_OK) {
        fprintf(stderr,
                "%s failed: %s (%" PRId32 ")\n",
                operation,
                mma_status_message(status),
                status);
    }
}

static int32_t wrapping_i32_pow(int32_t base, uint32_t exp) {
    uint32_t result = 1;
    uint32_t factor = (uint32_t)base;

    while (exp > 0) {
        if ((exp & 1U) != 0) {
            result *= factor;
        }
        exp >>= 1U;
        if (exp != 0) {
            factor *= factor;
        }
    }

    return (int32_t)result;
}

static int parse_i32_bytes(const uint8_t *data, size_t len, int32_t *output) {
    size_t i = 0;
    int negative = 0;
    uint32_t value = 0;
    uint32_t limit = INT32_MAX;

    if (len == 0) {
        return 0;
    }

    if (data[0] == '-' || data[0] == '+') {
        negative = data[0] == '-';
        limit = negative ? 2147483648U : 2147483647U;
        i = 1;
        if (i == len) {
            return 0;
        }
    }

    for (; i < len; ++i) {
        uint8_t digit;

        if (data[i] < '0' || data[i] > '9') {
            return 0;
        }

        digit = (uint8_t)(data[i] - '0');
        if (value > (limit - digit) / 10U) {
            return 0;
        }
        value = value * 10U + digit;
    }

    *output = negative
        ? (value == 2147483648U ? INT32_MIN : -(int32_t)value)
        : (int32_t)value;
    return 1;
}

static int parse_u32_bytes(const uint8_t *data, size_t len, uint32_t *output) {
    size_t i = 0;
    uint32_t value = 0;

    if (len == 0) {
        return 0;
    }

    if (data[0] == '+') {
        i = 1;
        if (i == len) {
            return 0;
        }
    } else if (data[0] == '-') {
        return 0;
    }

    for (; i < len; ++i) {
        uint8_t digit;

        if (data[i] < '0' || data[i] > '9') {
            return 0;
        }

        digit = (uint8_t)(data[i] - '0');
        if (value > (UINT32_MAX - digit) / 10U) {
            return 0;
        }
        value = value * 10U + digit;
    }

    *output = value;
    return 1;
}

static int32_t set_text_response(MMARouteResponse *response,
                                 const char *text,
                                 size_t text_len) {
    int32_t status;

    status = mma_response_set_status(response, MMA_OPCODE_OK, MMA_VERSION);
    if (status != MMA_STATUS_OK) {
        return status;
    }

    status = mma_response_add_option(response,
                                     (const uint8_t *)"content-type",
                                     12,
                                     (const uint8_t *)"text/plain",
                                     10);
    if (status != MMA_STATUS_OK) {
        return status;
    }

    return mma_response_set_payload(response, (const uint8_t *)text, text_len);
}

static int32_t echo_route(const MMARequest *request,
                          MMARouteResponse *response,
                          void *user_data) {
    const uint8_t *payload = NULL;
    size_t payload_len = 0;
    int32_t status;

    (void)user_data;

    status = mma_request_payload(request, &payload, &payload_len);
    if (status != MMA_STATUS_OK) {
        return status;
    }

    status = mma_response_set_status(response, MMA_OPCODE_OK, MMA_VERSION);
    if (status != MMA_STATUS_OK) {
        return status;
    }

    return mma_response_set_payload(response, payload, payload_len);
}

static int32_t pow_route(const MMARequest *request,
                         MMARouteResponse *response,
                         void *user_data) {
    const uint8_t *payload = NULL;
    size_t payload_len = 0;
    size_t options_count = 0;
    int32_t number = 1;
    uint32_t exponent = 2;
    char response_text[128];
    int written;
    int32_t status;

    (void)user_data;

    status = mma_request_payload(request, &payload, &payload_len);
    if (status != MMA_STATUS_OK) {
        return status;
    }
    if (!parse_i32_bytes(payload, payload_len, &number)) {
        number = 1;
    }

    status = mma_request_options_count(request, &options_count);
    if (status != MMA_STATUS_OK) {
        return status;
    }

    for (size_t i = 0; i < options_count; ++i) {
        const uint8_t *key = NULL;
        const uint8_t *value = NULL;
        size_t key_len = 0;
        size_t value_len = 0;

        status = mma_request_option(request, i, &key, &key_len, &value, &value_len);
        if (status != MMA_STATUS_OK) {
            return status;
        }

        if (key_len == 3 && memcmp(key, "pow", 3) == 0) {
            if (!parse_u32_bytes(value, value_len, &exponent)) {
                exponent = 2;
            }
            break;
        }
    }

    written = snprintf(response_text,
                       sizeof(response_text),
                       "%" PRId32 "^%" PRIu32 " = %" PRId32,
                       number,
                       exponent,
                       wrapping_i32_pow(number, exponent));
    if (written < 0 || (size_t)written >= sizeof(response_text)) {
        return MMA_STATUS_CALLBACK_ERROR;
    }

    return set_text_response(response, response_text, (size_t)written);
}

static int32_t register_route(MMAServer *server,
                              const char *route,
                              MMARouteCallback callback) {
    return mma_server_register_route(server,
                                     (const uint8_t *)route,
                                     strlen(route),
                                     callback,
                                     NULL);
}

int main(void) {
    MMAServerConfig config;
    MMAServer *server = NULL;
    uint16_t bound_port = 0;
    int32_t status;

    status = mma_server_config_default(&config);
    if (status != MMA_STATUS_OK) {
        print_status("mma_server_config_default", status);
        return 1;
    }

    config.bind_ip = "0.0.0.0";
    config.port = 8080;
    config.max_batch = 255;
    config.framer.max_message_length = 10U * 1000U * 1000U;
    config.framer.opcode_pos = 1;
    config.framer.version_pos = 4;
    config.framer.route_len_pos = 6;
    config.framer.options_count_pos = 7;
    config.framer.payload_order = 1;
    config.framer.options_order = 2;
    config.framer.route_order = 3;
    config.framer.options_key_first = 1;
    config.framer.header_length = 29;

    status = mma_server_create(&config, &server);
    if (status != MMA_STATUS_OK) {
        print_status("mma_server_create", status);
        return 1;
    }

    status = register_route(server, "GET", echo_route);
    if (status != MMA_STATUS_OK) {
        print_status("mma_server_register_route(GET)", status);
        mma_server_destroy(server);
        return 1;
    }

    status = register_route(server, "POW", pow_route);
    if (status != MMA_STATUS_OK) {
        print_status("mma_server_register_route(POW)", status);
        mma_server_destroy(server);
        return 1;
    }

    status = mma_server_start(server);
    if (status != MMA_STATUS_OK) {
        print_status("mma_server_start", status);
        mma_server_destroy(server);
        return 1;
    }

    status = mma_server_bound_port(server, &bound_port);
    if (status != MMA_STATUS_OK) {
        print_status("mma_server_bound_port", status);
        mma_server_destroy(server);
        return 1;
    }

    printf("C MMA server listens on 0.0.0.0:%" PRIu16 "\n", bound_port);
    printf("Routes: GET echoes payload, POW returns number^pow. Press Enter to stop.\n");
    (void)getchar();

    status = mma_server_stop(server);
    if (status != MMA_STATUS_OK) {
        print_status("mma_server_stop", status);
        mma_server_destroy(server);
        return 1;
    }

    mma_server_destroy(server);
    return 0;
}
