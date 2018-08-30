#include <errno.h>
#include <getopt.h>
#include <stdbool.h>
#include <stdio.h>

#include "../dyn_log.h"
#include "../hashkit/dyn_token.h"

static struct option long_options[] = {
    { "help",                 no_argument,        NULL,   'h' },
    { "outputkey",           no_argument,        NULL,   'k' },
    { "tokenfile",              required_argument,  NULL,   'o' },
    { "keyfile",       required_argument,  NULL,   'i' },
    { NULL,             0,                  NULL,    0  }
};

static char short_options[] = "hki:o:";

bool outputkey = false;
char * key_filename = NULL;
char * token_filename = NULL;
static void
print_usage()
{
    printf("Usage: dyno-hash-tool [-hk] -i <filename> -o <output filename>\n");
    printf("Read a key from input, hash key to create token, then output token to output.\n");
    printf("  -i <filename> or '-' for stdin (default)\n");
    printf("  -o <filename> or '-' for stdout (default)\n\n");
    printf("Options:\n");
    printf("  -h, --help             : this help\n");
    printf("  -k                     : include key in output\n\n");
    printf("Read a key from the input file and output the token to the output file. Default\n");
    printf("input and output are stdin and stdout, respectively. Input file must list one\n");
    printf("key per line. Output file will list one token per line that matches the line\n");
    printf("number of the input key.\n\n"); 
    printf("If the '-k' option is specified, then keys and tokens will output on alternating\n");
    printf("rows. Each key has 'KEY:' appended to the beginning of the line\n\n");
    printf("WARNING: dyno-hash-tool ONLY SUPPORTS MURMUR HASH (CURRENTLY)\n\n");
}

static int 
dn_get_options(int argc, char **argv)
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
            print_usage();
            return 1;

        case 'k':
            outputkey = true;
            break;

        case 'i':
            key_filename = optarg;
            break;

        case 'o':
            token_filename = optarg;
            break;

        default:
            printf("dynomite: invalid option -- '%c'", optopt);
            return 1;

        }
    }

    return 0;
}


int main(int argc, char **argv)
{
    log_init(5, NULL);
    int ret = dn_get_options(argc, argv);
    if (ret)
        exit(EINVAL);
    if (!key_filename)
        key_filename = "-";
    if (!token_filename)
        token_filename = "-";
    FILE *ifp, *ofp;
    char *line = NULL;
    size_t len = 0;
    ssize_t read;

    if (!strcmp(key_filename, "-"))
        ifp = stdin;
    else {
        log_debug(LOG_VERB, "opening input stream %s", key_filename);
        ifp = fopen(key_filename, "r");
    }

    if (ifp == NULL) {
        log_error("could not open input stream");
        exit(EXIT_FAILURE);
    }

    if (!strcmp(token_filename, "-"))
        ofp = stdout;
    else {
        log_debug(LOG_VERB, "opening output stream %s", key_filename);
        ofp = fopen(token_filename, "w");
    }

    if (ofp == NULL) {
        log_error("could not open input stream");
        exit(EXIT_FAILURE);
    }

    while ((read = getline(&line, &len, ifp)) != -1) {
        if (line[read-1] == '\n') {
            line[read-1] = '\0';
            read--;
        }
        struct dyn_token d;
        init_dyn_token(&d);
        hash_murmur(line, read, &d);
        log_debug(LOG_VERB, "KEY (%s) Token: %lu", line, *d.mag);
        if (outputkey)
            fprintf(ofp, "KEY:%s\n", line);
        fprintf(ofp, "%lu\n", *d.mag);
    }

    fclose(ofp);
    fclose(ifp);
    free(line);
    exit(EXIT_SUCCESS);
}
