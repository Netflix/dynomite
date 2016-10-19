/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */ 

/*
 * twemproxy - A fast and lightweight proxy for memcached protocol.
 * Copyright (C) 2011 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <getopt.h>
#include <fcntl.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/utsname.h>

#include "dyn_core.h"
#include "dyn_conf.h"
#include "dyn_signal.h"
#include "dyn_asciilogo.h"

#define DN_CONF_PATH        "conf/dynomite.yml"

#define DN_LOG_DEFAULT      LOG_NOTICE
#define DN_LOG_MIN          LOG_EMERG
#define DN_LOG_MAX          LOG_PVERB
#define DN_LOG_PATH         NULL

#define DN_STATS_PORT       STATS_PORT
#define DN_STATS_ADDR       STATS_ADDR
#define DN_STATS_INTERVAL   STATS_INTERVAL

#define DN_ENTROPY_PORT		ENTROPY_PORT
#define DN_ENTROPY_ADDR		ENTROPY_ADDR

#define DN_PID_FILE         NULL

#define DN_MBUF_SIZE        MBUF_SIZE
#define DN_MBUF_MIN_SIZE    MBUF_MIN_SIZE
#define DN_MBUF_MAX_SIZE    MBUF_MAX_SIZE

#define DN_ALLOC_MSGS			ALLOC_MSGS
#define DN_MIN_ALLOC_MSGS	MIN_ALLOC_MSGS
#define DN_MAX_ALLOC_MSGS	MAX_ALLOC_MSGS

static int show_help;
static int show_version;
static int test_conf;
static int admin_opt = 0;
static int daemonize;
static bool enable_gossip = false;
static int describe_stats;

static struct option long_options[] = {
    { "help",                 no_argument,        NULL,   'h' },
    { "version",              no_argument,        NULL,   'V' },
    { "test-conf",            no_argument,        NULL,   't' },
    { "daemonize",            no_argument,        NULL,   'd' },
    { "describe-stats",       no_argument,        NULL,   'D' },
    { "gossip",               no_argument,        NULL,   'g' },
    { "verbosity",            required_argument,  NULL,   'v' },
    { "output",               required_argument,  NULL,   'o' },
    { "conf-file",            required_argument,  NULL,   'c' },
    { "stats-port",           required_argument,  NULL,   's' },
    { "stats-interval",       required_argument,  NULL,   'i' },
    { "stats-addr",           required_argument,  NULL,   'a' },
    { "pid-file",             required_argument,  NULL,   'p' },
    { "mbuf-size",            required_argument,  NULL,   'm' },
    { "max-msgs",             required_argument,  NULL,   'M' },
    { "admin-operation",      required_argument,  NULL,   'x' },
    { "admin-param",          required_argument,  NULL,   'y' },
    { NULL,             0,                  NULL,    0  }
};

static char short_options[] = "hVtdDgv:o:c:s:i:a:p:m:M:x:y";

/**
 * Daemonize dynomite and redirect stdin, stdout and stderr to /dev/null.
 * @param[in] dump_core If set to 0 then dynomite tries to chdir to /.
 * @return rstatus_t Return status code.
 */
static rstatus_t
dn_daemonize(int dump_core)
{
    rstatus_t status;
    pid_t pid, sid;
    int fd;

    pid = fork();
    switch (pid) {
    case -1:
        log_error("fork() failed: %s", strerror(errno));
        return DN_ERROR;

    case 0:
        break;

    default:
        /* parent terminates */
        _exit(0);
    }

    /* 1st child continues and becomes the session leader */

    sid = setsid();
    if (sid < 0) {
        log_error("setsid() failed: %s", strerror(errno));
        return DN_ERROR;
    }

    if (signal(SIGHUP, SIG_IGN) == SIG_ERR) {
        log_error("signal(SIGHUP, SIG_IGN) failed: %s", strerror(errno));
        return DN_ERROR;
    }

    pid = fork();
    switch (pid) {
    case -1:
        log_error("fork() failed: %s", strerror(errno));
        return DN_ERROR;

    case 0:
        break;

    default:
        /* 1st child terminates */
        _exit(0);
    }

    /* 2nd child continues */

    /* change working directory */
    if (dump_core == 0) {
        status = chdir("/");
        if (status < 0) {
            log_error("chdir(\"/\") failed: %s", strerror(errno));
            return DN_ERROR;
        }
    }

    /* clear file mode creation mask */
    umask(0);

    /* redirect stdin, stdout and stderr to "/dev/null" */

    fd = open("/dev/null", O_RDWR);
    if (fd < 0) {
        log_error("open(\"/dev/null\") failed: %s", strerror(errno));
        return DN_ERROR;
    }

    status = dup2(fd, STDIN_FILENO);
    if (status < 0) {
        log_error("dup2(%d, STDIN) failed: %s", fd, strerror(errno));
        close(fd);
        return DN_ERROR;
    }

    status = dup2(fd, STDOUT_FILENO);
    if (status < 0) {
        log_error("dup2(%d, STDOUT) failed: %s", fd, strerror(errno));
        close(fd);
        return DN_ERROR;
    }

    status = dup2(fd, STDERR_FILENO);
    if (status < 0) {
        log_error("dup2(%d, STDERR) failed: %s", fd, strerror(errno));
        close(fd);
        return DN_ERROR;
    }

    if (fd > STDERR_FILENO) {
        status = close(fd);
        if (status < 0) {
            log_error("close(%d) failed: %s", fd, strerror(errno));
            return DN_ERROR;
        }
    }

    return DN_OK;
}

/**
 * Print start messages.
 * @param[in] nci Dynomite instance
 */
static void
dn_print_run(struct instance *nci)
{
    int status;
    struct utsname name;

    status = uname(&name);
    if (status < 0) {
        loga("dynomite-%s started on pid %d", VERSION, nci->pid);
    } else {
        loga("dynomite-%s built for %s %s %s started on pid %d",
             VERSION, name.sysname, name.release, name.machine, nci->pid);
    }

    loga("run, rabbit run / dig that hole, forget the sun / "
         "and when at last the work is done / don't sit down / "
         "it's time to dig another one");

    loga("%s",ascii_logo);
}

static void
dn_print_done(void)
{
    loga("done, rabbit done");
}

static void
dn_show_usage(void)
{
    log_stderr(
        "Usage: dynomite [-?hVdDt] [-v verbosity level] [-o output file]" CRLF
        "                  [-c conf file] [-s stats port] [-a stats addr]" CRLF
        "                  [-i stats interval] [-p pid file] [-m mbuf size]" CRLF
        "                  [-M max alloc messages]" CRLF
        "");
    log_stderr(
        "Options:" CRLF
        "  -h, --help             : this help" CRLF
        "  -V, --version          : show version and exit" CRLF
        "  -t, --test-conf        : test configuration for syntax errors and exit" CRLF
        "  -g, --gossip           : enable gossip (default: disable)" CRLF
        "  -d, --daemonize        : run as a daemon" CRLF
        "  -D, --describe-stats   : print stats description and exit");
    log_stderr(
        "  -v, --verbosity=N            : set logging level (default: %d, min: %d, max: %d)" CRLF
        "  -o, --output=S               : set logging file (default: %s)" CRLF
        "  -c, --conf-file=S            : set configuration file (default: %s)" CRLF
        "  -s, --stats-port=N           : set stats monitoring port (default: %d)" CRLF
        "  -a, --stats-addr=S           : set stats monitoring ip (default: %s)" CRLF
        "  -i, --stats-interval=N       : set stats aggregation interval in msec (default: %d msec)" CRLF
        "  -p, --pid-file=S             : set pid file (default: %s)" CRLF
        "  -m, --mbuf-size=N            : set size of mbuf chunk in bytes (default: %d bytes)" CRLF
        "  -M, --max-msgs=N             : set max number of messages to allocate (default: %d)" CRLF
        "  -x, --admin-operation=N      : set size of admin operation (default: %d)" CRLF
        "",
        DN_LOG_DEFAULT, DN_LOG_MIN, DN_LOG_MAX,
        DN_LOG_PATH != NULL ? DN_LOG_PATH : "stderr",
        DN_CONF_PATH,
        DN_STATS_PORT, DN_STATS_ADDR, DN_STATS_INTERVAL,
        DN_PID_FILE != NULL ? DN_PID_FILE : "off",
        DN_MBUF_SIZE, DN_ALLOC_MSGS,
        0);
}

static rstatus_t
dn_create_pidfile(struct instance *nci)
{
    char pid[DN_UINTMAX_MAXLEN];
    int fd, pid_len;
    ssize_t n;

    fd = open(nci->pid_filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        log_error("opening pid file '%s' failed: %s", nci->pid_filename,
                  strerror(errno));
        return DN_ERROR;
    }
    nci->pidfile = 1;

    pid_len = dn_snprintf(pid, DN_UINTMAX_MAXLEN, "%d", nci->pid);

    n = dn_write(fd, pid, pid_len);
    if (n < 0) {
        log_error("write to pid file '%s' failed: %s", nci->pid_filename,
                  strerror(errno));
        return DN_ERROR;
    }

    close(fd);

    return DN_OK;
}

static void
dn_remove_pidfile(struct instance *nci)
{
    int status;

    status = unlink(nci->pid_filename);
    if (status < 0) {
        log_error("unlink of pid file '%s' failed, ignored: %s",
                  nci->pid_filename, strerror(errno));
    }
}

/**
 * Set the dynomite instance properties to the default values, except the
 * hostname which is set via gethostname().
 * @param nci dynomite instance
 */
static void
dn_set_default_options(struct instance *nci)
{
    int status;

    nci->ctx = NULL;

    nci->log_level = DN_LOG_DEFAULT;
    nci->log_filename = DN_LOG_PATH;

    nci->conf_filename = DN_CONF_PATH;

    nci->stats_port = DN_STATS_PORT;
    nci->stats_addr = DN_STATS_ADDR;
    nci->stats_interval = DN_STATS_INTERVAL;

    nci->entropy_port = DN_ENTROPY_PORT;
    nci->entropy_addr = DN_ENTROPY_ADDR;

    status = dn_gethostname(nci->hostname, DN_MAXHOSTNAMELEN);
    if (status < 0) {
        log_warn("gethostname failed, ignored: %s", strerror(errno));
        dn_snprintf(nci->hostname, DN_MAXHOSTNAMELEN, "unknown");
    }
    nci->hostname[DN_MAXHOSTNAMELEN - 1] = '\0';

    nci->mbuf_chunk_size = DN_MBUF_SIZE;

    nci->alloc_msgs_max = DN_ALLOC_MSGS;

    nci->pid = (pid_t)-1;
    nci->pid_filename = NULL;
    nci->pidfile = 0;
}

/**
 * Parse the command line options.
 * @param argc argument count
 * @param argv argument values
 * @param nci dynomite instance
 * @return return status
 */
static rstatus_t
dn_get_options(int argc, char **argv, struct instance *nci)
{
    int c, value;

    opterr = 0;

    for (;;) {
        c = getopt_long(argc, argv, short_options, long_options, NULL);
        if (c == -1) {
            /* no more options */
            break;
        }

        switch (c) {
        case 'h':
            show_version = 1;
            show_help = 1;
            break;

        case 'V':
            show_version = 1;
            break;

        case 't':
            test_conf = 1;
            nci->log_level = 11;
            break;

        case 'd':
            daemonize = 1;
            break;

        case 'D':
            describe_stats = 1;
            show_version = 1;
            break;

        case 'g':
            enable_gossip = true;
            break;

        case 'v':
            value = dn_atoi(optarg, strlen(optarg));
            if (value < 0) {
                log_stderr("dynomite: option -v requires a number");
                return DN_ERROR;
            }
            nci->log_level = value;
            break;

        case 'o':
            nci->log_filename = optarg;
            break;

        case 'c':
            nci->conf_filename = optarg;
            break;

        case 's':
            value = dn_atoi(optarg, strlen(optarg));
            if (value < 0) {
                log_stderr("dynomite: option -s requires a number");
                return DN_ERROR;
            }
            if (!dn_valid_port(value)) {
                log_stderr("dynomite: option -s value %d is not a valid "
                           "port", value);
                return DN_ERROR;
            }

            nci->stats_port = (uint16_t)value;
            break;

        case 'i':
            value = dn_atoi(optarg, strlen(optarg));
            if (value < 0) {
                log_stderr("dynomite: option -i requires a number");
                return DN_ERROR;
            }

            nci->stats_interval = value;
            break;

        case 'a':
            nci->stats_addr = optarg;
            break;

        case 'p':
            nci->pid_filename = optarg;
            break;

        case 'm':
            value = dn_atoi(optarg, strlen(optarg));
            if (value <= 0) {
                log_stderr("dynomite: option -m requires a non-zero number");
                return DN_ERROR;
            }

            if (value < DN_MBUF_MIN_SIZE || value > DN_MBUF_MAX_SIZE) {
                log_stderr("dynomite: mbuf chunk size must be between %zu and"
                           " %zu bytes", DN_MBUF_MIN_SIZE, DN_MBUF_MAX_SIZE);
                return DN_ERROR;
            }

            if ((value / 16) * 16 != value) {
               log_stderr("dynomite: mbuf chunk size must be a multiple of 16");
               return DN_ERROR;
            }

            nci->mbuf_chunk_size = (size_t)value;
            break;

        case 'M':
            value = dn_atoi(optarg, strlen(optarg));
            if (value <= 0) {
                log_stderr("dynomite: option -M requires a non-zero number");
                return DN_ERROR;
            }

            if (value < DN_MIN_ALLOC_MSGS || value > DN_MAX_ALLOC_MSGS) {
                log_stderr("dynomite: max allocated messages buffer must be between %zu and"
                           " %zu messages", DN_MIN_ALLOC_MSGS, DN_MAX_ALLOC_MSGS);
                return DN_ERROR;
            }

            nci->alloc_msgs_max = (size_t)value;

        	break;

        case 'x':
            value = dn_atoi(optarg, strlen(optarg));
            if (value <= 0) {
               log_stderr("dynomite: option -x requires a non-zero number");
               return DN_ERROR;
            }
            admin_opt = value;

            break;
        case '?':
            switch (optopt) {
            case 'o':
            case 'c':
            case 'p':
                log_stderr("dynomite: option -%c requires a file name",
                           optopt);
                break;

            case 'm':
            case 'M':
            case 'v':
            case 's':
            case 'i':
                log_stderr("dynomite: option -%c requires a number", optopt);
                break;

            case 'a':
                log_stderr("dynomite: option -%c requires a string", optopt);
                break;

            default:
                log_stderr("dynomite: invalid option -- '%c'", optopt);
                break;
            }
            return DN_ERROR;

        default:
            log_stderr("dynomite: invalid option -- '%c'", optopt);
            return DN_ERROR;

        }
    }

    return DN_OK;
}

/**
 * Test the dynomite.yml configuration file's syntax.
 * @param[in] nci Dynomite instance
 * @return bool true if the configuration file has a valid syntax or false if
 *         syntax is invalid
 */
static bool
dn_test_conf(struct instance *nci)
{
    struct conf *cf;

    cf = conf_create(nci->conf_filename);
    if (cf == NULL) {
        log_stderr("dynomite: configuration file '%s' syntax is invalid",
                   nci->conf_filename);
        return false;
    }

    conf_destroy(cf);

    log_stderr("dynomite: configuration file '%s' syntax is valid",
               nci->conf_filename);
    return true;
}

/**
 * Final setup before running Dynomite. Initialize logging, daemonize dynomite,
 * get the PID, and initialize POSIX signal handling.
 * @param[in] nci Dynomite instance.
 * @return rstatus_t Return status code.
 */
static rstatus_t
dn_pre_run(struct instance *nci)
{
    rstatus_t status;

    status = log_init(nci->log_level, nci->log_filename);
    if (status != DN_OK) {
        return status;
    }

    if (daemonize) {
        status = dn_daemonize(1);
        if (status != DN_OK) {
            return status;
        }
    }

    nci->pid = getpid();

    status = signal_init();
    if (status != DN_OK) {
        return status;
    }

    if (nci->pid_filename) {
        status = dn_create_pidfile(nci);
        if (status != DN_OK) {
            return status;
        }
    }

    dn_print_run(nci);

    return DN_OK;
}

/**
 * Cleanup when shutting down Dynomite. Delete the PID, print a done message,
 * and close the logging file descriptor.
 * @param[in] nci Dynomite instance.
 */
static void
dn_post_run(struct instance *nci)
{
    if (nci->pidfile) {
        dn_remove_pidfile(nci);
    }

    signal_deinit();

    dn_print_done();

    log_deinit();
}

/**
 * Call method to initialize buffers, messages and connections. Then start the
 * core dynomite loop to process messsages. When dynomite is shutting down, call
 * method to deinitialize buffers, messages and connections.
 * @param[in] nci Dynomite instance.
 * @return rstatus_t Return status code.
 */
static rstatus_t
dn_run(struct instance *nci)
{
    rstatus_t status;

    THROW_STATUS(core_start(nci));

    struct context *ctx = nci->ctx;
    ctx->enable_gossip = enable_gossip;
    ctx->admin_opt = (unsigned)admin_opt;

    if (!ctx->enable_gossip)
    	ctx->dyn_state = NORMAL;

    /* run rabbit run */
    for (;;) {
        status = core_loop(ctx);
        if (status != DN_OK) {
            break;
        }
    }

    core_stop(ctx);
    return DN_OK;
}

/**
 * Set unlimited core dump resource limits.
 */
static void
dn_coredump_init(void)
{
   struct rlimit core_limits;
   core_limits.rlim_cur = core_limits.rlim_max = RLIM_INFINITY;
   setrlimit(RLIMIT_CORE, &core_limits);
}

int
main(int argc, char **argv)
{
    rstatus_t status;
    struct instance nci;

    dn_coredump_init();
    dn_set_default_options(&nci);

    status = dn_get_options(argc, argv, &nci);
    if (status != DN_OK) {
        dn_show_usage();
        exit(1);
    }

    if (show_version) {
        log_stderr("This is dynomite-%s" CRLF, VERSION);
        if (show_help) {
            dn_show_usage();
        }

        if (describe_stats) {
            stats_describe();
        }

        exit(0);
    }

    if (test_conf) {
        if (!dn_test_conf(&nci)) {
            exit(1);
        }
        exit(0);
    }

    status = dn_pre_run(&nci);
    if (status != DN_OK) {
        dn_post_run(&nci);
        exit(1);
    }

    status = dn_run(&nci);
    IGNORE_RET_VAL(status);

    dn_post_run(&nci);

    exit(1);
}
