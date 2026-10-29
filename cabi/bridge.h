#ifndef MMA_GO_BRIDGE_H
#define MMA_GO_BRIDGE_H

#include "mma.h"
#include <stdint.h>

void *mma_handle_to_ptr(uintptr_t handle);
uintptr_t mma_ptr_to_handle(const void *ptr);

int32_t mma_server_register_go_route(
    MMA_Server *server,
    const uint8_t *route,
    size_t route_len,
    void *user_data
);

int32_t mma_server_register_go_channel_handler(
    MMA_Server *server,
    const uint8_t *route,
    size_t route_len,
    void *user_data
);

#endif
