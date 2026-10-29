//go:build cgo

#include "bridge.h"

extern int32_t go_mma_route_callback(
    const MMA_Request *request,
    MMA_Response *response,
    void *user_data
);

extern int32_t go_mma_channel_callback(
    const MMA_Request *request,
    uint32_t connection_id,
    void *user_data
);

static MMA_Response mma_go_route_trampoline(
    MMA_Request request,
    MMA_Response response,
    const void *user_data
) {
    MMA_Response result = response;
    (void)go_mma_route_callback(&request, &result, (void *)user_data);
    return result;
}

static int32_t mma_go_channel_trampoline(
    const MMA_Request *request,
    uint32_t connection_id,
    const void *user_data
) {
    return go_mma_channel_callback(request, connection_id, (void *)user_data);
}

void *mma_handle_to_ptr(uintptr_t handle) {
    return (void *)handle;
}

uintptr_t mma_ptr_to_handle(const void *ptr) {
    return (uintptr_t)ptr;
}

int32_t mma_server_register_go_route(
    MMA_Server *server,
    const uint8_t *route,
    size_t route_len,
    void *user_data
) {
    return mma_server_register_route(
        server,
        route,
        route_len,
        mma_go_route_trampoline,
        user_data
    );
}

int32_t mma_server_register_go_channel_handler(
    MMA_Server *server,
    const uint8_t *route,
    size_t route_len,
    void *user_data
) {
    return mma_server_register_channel_handler(
        server,
        route,
        route_len,
        mma_go_channel_trampoline,
        user_data
    );
}
