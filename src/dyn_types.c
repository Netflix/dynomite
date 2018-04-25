#include "dyn_types.h"
#include "stdio.h"

#define OBJECT_MAGIC 0xdead

void
cleanup_charptr(char **ptr) {
    if (*ptr)
        free(*ptr);
}

void
init_object(struct object *obj, object_type_t type, func_print_t print)
{
    obj->magic = OBJECT_MAGIC;
    obj->type = type;
    obj->func_print = print;
}

char*
print_obj(const void *ptr)
{
    const object_t *obj = (const object_t *)ptr;
    static char buffer[PRINT_BUF_SIZE];
    if (obj == NULL) {
        snprintf(buffer, PRINT_BUF_SIZE, "<NULL>");
        return buffer;
    }
    if (obj->magic != OBJECT_MAGIC) {
        snprintf(buffer, PRINT_BUF_SIZE, "addr:%p <CORRUPTION> MAGIC NUMBER 0x%x", obj, obj->magic);
        return buffer;
    }
    if ((obj->type >= 0) && (obj->type < OBJ_LAST)) {
        return obj->func_print(obj);
    } else {
        snprintf(buffer, PRINT_BUF_SIZE, "addr:%p <CORRUPTION> INVALID TYPE %d", obj, obj->type);
        return buffer;
    }
}
