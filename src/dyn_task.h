#include "dyn_rbtree.h"
#include "dyn_types.h"
#include "dyn_core.h"

struct task;

/* handler that takes 1 argument */
typedef void (*task_handler_1)(void *arg1);

/* Initialize the task manager */
rstatus_t task_mgr_init(void);

/* creates a new task and adds to the internal datastructure
 * handler1 : a handler function that takes 1 arguemtn
 * arg1 : the argument that will be sent back to handler1
 * timeout : time in msec after which this task should get fired
 */
struct task *schedule_task_1(task_handler_1 handler1, void *arg1, msec_t timeout);

/* Returns the time in msec to the next task */
msec_t time_to_next_task(void);

/* Execute expired tasks one after other calling individual handlers */
/* limit = 0 (execute all expired tasks)
 *       > 0 (upto limit tasks)
 */
void execute_expired_tasks(uint32_t limit);

/* Cancel the provided task. The caller should keep track of the tasks scheduled
 * and use it to cancel */
void cancel_task(struct task *task);
