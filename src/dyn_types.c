#include "dyn_types.h"

void
cleanup_charptr(const char **ptr) {
    if (*ptr)
        free(*ptr);
}
