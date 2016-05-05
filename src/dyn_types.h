#pragma once
typedef uint64_t msgid_t;
typedef uint64_t msec_t;
typedef uint64_t usec_t;

typedef enum {
    SECURE_OPTION_NONE,
    SECURE_OPTION_RACK,
    SECURE_OPTION_DC,
    SECURE_OPTION_ALL,
}secure_server_option_t;
