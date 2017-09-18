#include "dyn_types.h"

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

int
print_obj_arginfo(const struct printf_info *info, size_t n, int *argtypes)
{
      /* We always take exactly one argument and this is a pointer to the
       *      structure.. */
    if (n > 0)
        argtypes[0] = PA_POINTER;
    return 1;
}

int
print_obj(FILE *stream, const struct printf_info *info, const void *const *args)
{
    const object_t *obj;

    obj = *((const object_t **) (args[0]));
    if (obj == NULL) {
        return fprintf(stream, "<NULL>");
    }
    if (obj->magic != OBJECT_MAGIC) {
        return fprintf(stream, "addr:%p <CORRUPTION> MAGIC NUMBER 0x%x", obj, obj->magic);
    }
    if ((obj->type >= 0) && (obj->type < OBJ_LAST))
       return obj->func_print(stream, obj);
    else
        return fprintf(stream, "addr:%p <CORRUPTION> INVALID TYPE %d", obj, obj->type);
}
