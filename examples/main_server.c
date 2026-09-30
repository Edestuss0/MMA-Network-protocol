#include "mma.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

static void print_status(const char *operation, int32_t status) {
    if (status != MMA_STATUS_OK) {
        fprintf(stderr, "%s failed: %s (%" PRId32 ")\n",
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

static int is_valid_utf8(const uint8_t *data, size_t len) {
    size_t i = 0;

    while (i < len) {
        uint8_t byte = data[i];

        if (byte <= 0x7F) {
            ++i;
        } else if ((byte & 0xE0) == 0xC0) {
            if (i + 1 >= len || byte < 0xC2 || (data[i + 1] & 0xC0) != 0x80) {
                return 0;
            }
            i += 2;
        } else if ((byte & 0xF0) == 0xE0) {
            if (i + 2 >= len ||
                (data[i + 1] & 0xC0) != 0x80 ||
                (data[i + 2] & 0xC0) != 0x80 ||
                (byte == 0xE0 && data[i + 1] < 0xA0) ||
                (byte == 0xED && data[i + 1] >= 0xA0)) {
                return 0;
            }
            i += 3;
        } else if ((byte & 0xF8) == 0xF0) {
            if (i + 3 >= len ||
                (data[i + 1] & 0xC0) != 0x80 ||
                (data[i + 2] & 0xC0) != 0x80 ||
                (data[i + 3] & 0xC0) != 0x80 ||
                (byte == 0xF0 && data[i + 1] < 0x90) ||
                (byte == 0xF4 && data[i + 1] >= 0x90) ||
                byte > 0xF4) {
                return 0;
            }
            i += 4;
        } else {
            return 0;
        }
    }

    return 1;
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

    if (negative) {
        *output = value == 2147483648U ? INT32_MIN : -(int32_t)value;
    } else {
        *output = (int32_t)value;
    }

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

static int32_t get_route(
    const MMARequest *request,
    MMAResponseBuilder *response,
    void *user_data
) {
    const uint8_t *payload = NULL;
    size_t payload_len = 0;
    int32_t status;

    (void)user_data;

    status = mma_request_payload(request, &payload, &payload_len);
    if (status != MMA_STATUS_OK) {
        return status;
    }

    status = mma_response_builder_set_status(response, 1, 1);
    if (status != MMA_STATUS_OK) {
        return status;
    }

    return mma_response_builder_set_payload(response, payload, payload_len);
}

static int32_t pow_route(
    const MMARequest *request,
    MMAResponseBuilder *response,
    void *user_data
) {
    const uint8_t *payload = NULL;
    size_t payload_len = 0;
    size_t options_count = 0;
    int32_t number = 1;
    uint32_t pow_value = 2;
    int32_t calculated;
    char response_text[128];
    int written;
    int32_t status;

    (void)user_data;

    status = mma_request_payload(request, &payload, &payload_len);
    if (status != MMA_STATUS_OK) {
        return status;
    }

    if (!is_valid_utf8(payload, payload_len)) {
        number = 2;
    } else if (!parse_i32_bytes(payload, payload_len, &number)) {
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
            if (!parse_u32_bytes(value, value_len, &pow_value)) {
                pow_value = 2;
            }
            break;
        }
    }

    calculated = wrapping_i32_pow(number, pow_value);
    written = snprintf(response_text,
                       sizeof(response_text),
                       "%" PRId32 "^%" PRIu32 " = %" PRId32,
                       number,
                       pow_value,
                       calculated);
    if (written < 0 || (size_t)written >= sizeof(response_text)) {
        return MMA_STATUS_CALLBACK_ERROR;
    }

    status = mma_response_builder_set_status(response, 1, 1);
    if (status != MMA_STATUS_OK) {
        return status;
    }

    return mma_response_builder_set_payload(response,
                                            (const uint8_t *)response_text,
                                            (size_t)written);
}

int main(void) {
    MMAServerConfig config;
    MMAServer *server = NULL;
    int32_t status;

    status = mma_server_config_default(&config);
    if (status != MMA_STATUS_OK) {
        print_status("mma_server_config_default", status);
        return 1;
    }

    config.bind_ip = "0.0.0.0";
    config.port = 8080;
    config.max_batch = 255;

    config.framer.opcode_pos = 1;
    config.framer.version_pos = 4;
    config.framer.max_message_length = 10U * 1000U * 1000U;
    config.framer.route_len_pos = 6;
    config.framer.options_count_pos = 7;
    config.framer.route_order = 3;
    config.framer.options_order = 2;
    config.framer.payload_order = 1;
    config.framer.options_key_first = 1;
    config.framer.header_length = 29;

    status = mma_server_create(&config, &server);
    if (status != MMA_STATUS_OK) {
        print_status("mma_server_create", status);
        return 1;
    }

    status = mma_server_register_route(server, (const uint8_t *)"GET", 3, get_route, NULL);
    if (status != MMA_STATUS_OK) {
        print_status("mma_server_register_route(GET)", status);
        mma_server_destroy(server);
        return 1;
    }

    status = mma_server_register_route(server, (const uint8_t *)"POW", 3, pow_route, NULL);
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

    printf("C MMA server listens on 0.0.0.0:8080\n");
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
