#include <stdint.h>
#include <stddef.h>
#include <mma.h>

#if defined(_WIN32)
#define MMA_GO_API __declspec(dllexport)
#else
#define MMA_GO_API __attribute__((visibility("default")))
#endif

typedef int32_t (*MMA_GoRouteCallback)(
    const MMA_Request *request,
    MMA_Response *response,
    const void *user_data
);

static MMA_GoRouteCallback go_route_callback = NULL;

static MMA_Response route_callback_trampoline(
    MMA_Request request,
    MMA_Response response,
    const void *user_data
) {
    static const uint8_t fallback_message[] = "Go route callback failed";

    if (go_route_callback == NULL ||
        go_route_callback(&request, &response, user_data) != MMA_OK) {
        response = mma_response_set_status(response, MMA_RESPONSE_INTERNAL_ERROR);
        response = mma_response_set_payload(
            response,
            fallback_message,
            sizeof(fallback_message) - 1
        );
    }
    return response;
}

MMA_GO_API int32_t mma_go_bridge_set_route_callback(MMA_GoRouteCallback callback) {
    if (callback == NULL) {
        return MMA_INVALID_ARGUMENT;
    }
    go_route_callback = callback;
    return MMA_OK;
}

MMA_GO_API int32_t mma_go_server_register_route(
    MMA_Server *server,
    const uint8_t *route,
    size_t route_len,
    const void *user_data
) {
    return mma_server_register_route(
        server,
        route,
        route_len,
        route_callback_trampoline,
        user_data
    );
}
