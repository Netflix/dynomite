
#include "dyn_error.h"


char *dn_strerror(err_t err) {
    switch(err) {
        case DN_ENOHOST:
            return "No host found for given key";
        case DN_EHOST_DOWN:
            return "Could not connect to remote host";
        default:
            return strerror(err);
    }
}
