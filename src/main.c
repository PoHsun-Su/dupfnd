#include <stdio.h>
#include <stdlib.h>
#include <argp.h>

#include "util.h"
#include "list.h"
#include "rbtree.h"
#include "dupfnd.h"


const char *argp_program_version =
    "dupfnd ver.0.1a";
const char *argp_program_bug_address =
    "<bobstart37@gmail.com>";

/* Program documentation. */
static char doc[] =
    "Just a duplicatd file finder\n";

/* A description of the arguments we accept. */
static char args_doc[] = "directory [directory2 ...]";

/* Keys for options without short-options. */
#define OPT_NOSUMMARY   1            /* --no-summary   */
#define OPT_NOLISTFILE  2            /* --no-listfiles */

/* The options we understand. */
static struct argp_option options[] = {
    {"recursive",        'r',             0,              0, "Search recursively" },
    {"threads",          'j',             "[0-9]+",       0, "How many threads run concurrently" },
    {"crcmethod",        'c',             "[sw|sse|gpu]", 0, "The method to calculate CRC32" },
    {"no-summary",        OPT_NOSUMMARY,  0,              0, "Do not display the summary" },
    {"no-listfiles",      OPT_NOLISTFILE, 0,              0, "Do not display duplicated files" },
    {"show-sizes",       'z',             0,              0, "Show file size" },
    {"show-groupnum",    'g',             0,              0, "Display group number" },
    {"silent",           's',             0,              0, "Don't produce any output" },
    {"skip-empty-files", 'e',             0,              0, "Skip zero-byte files" },
    // {"depth",        'd', 0,              0, "Search depth" },
    // {"progress",     'p', 0,              0, "Print the progress" },
    { 0 }
};

/* Used by main to communicate with parse_opt. */
struct arguments {
    char *dir;                   /* directory */
    char **moreDirs;             /* [directory2 ...] */
    int silent;
    // int progress;
    int crcmethod;
    int threads;
    int recursive;
    int show_summary;
    int show_files;
    int show_size;
    int show_groupnum;
    int skipemptyfiles;
};

/* Parse a single option. */
static error_t
parse_opt (int key, char *arg, struct argp_state *state) {
    /* Get the input argument from argp_parse, which we
       know is a pointer to our arguments structure. */
    struct arguments *arguments = state->input;

    switch (key) {
        case 'e':
            arguments->skipemptyfiles = 1;
            break;
        case 'r':
            arguments->recursive = 1;
            break;
        case 's':
            arguments->silent = 1;
            break;
        case OPT_NOSUMMARY:
            arguments->show_summary = 0;
            break;
        case OPT_NOLISTFILE:
            arguments->show_files = 0;
            break;
        case 'g':
            arguments->show_groupnum = 1;
            break;
        case 'z':
            arguments->show_size = 1;
            break;
        // case 'p':
        //     arguments->progress = 1;
        //     break;
        case 'j':
            arguments->threads = arg ? atoi (arg) : 1;
            break;
        case 'c':
            if (strlen(arg)) {
                if (0 == strcasecmp("sse", arg))
                    arguments->crcmethod = FLAG_CRC_SSE;
                else
                    arguments->crcmethod = FLAG_CRC_SW;
            }
            break;
        case ARGP_KEY_NO_ARGS:
            argp_usage(state);
            break;
        case ARGP_KEY_ARG:
            arguments->dir = arg;
            arguments->moreDirs = &state->argv[state->next];
            state->next = state->argc;
            break;

        default:
            return ARGP_ERR_UNKNOWN;
    }
    return 0;
}

/* Our argp parser. */
static struct argp argp = { options, parse_opt, args_doc, doc };

int main (int argc, char **argv) {
    int nDir = 1;
    int i, j;
    struct arguments args;

    /* Default values. */
    args.silent = 0;
    args.show_summary = 1;
    args.show_files = 1;
    args.show_size = 0;
    args.show_groupnum = 0;
    // args.progress = 0;
    args.crcmethod = FLAG_CRC_SW;
    args.threads = 1;
    args.recursive = 0;
    args.skipemptyfiles = 0;

    argp_parse (&argp, argc, argv, 0, 0, &args);
    // argp_help(&argp, stdout, ARGP_HELP_USAGE, args_doc);

    if (1 == args.silent) {
        // args.progress = 0;
        args.show_summary = 0;
        args.show_groupnum = 0;
        args.show_files = 0;
        args.show_size = 0;
    }

    char **dirs = NULL;
    char actualabspath[PATH_MAX + 1] = {0};

    /* allocate only one pointer to pointer, will realloc later if needed */
    dirs = (char **)malloc(1 * sizeof(char *));
    if (strlen(args.dir) > PATH_MAX) {
        fprintf(stderr, "Error. file path too long\n");
        return -1;
    }

    realpath(args.dir, actualabspath);
    dirs[0] = strdup(actualabspath);

    if (args.moreDirs[0]) {
        for (j = 0; args.moreDirs[j]; j++) {
            dirs = (char **)realloc(dirs, (j + 1) * sizeof(char *));
            if (strlen(args.moreDirs[j]) > PATH_MAX) {
                fprintf(stderr, "Error. file path too long\n");
                return -1;
            }
            realpath(args.moreDirs[j], actualabspath);
            dirs[j + 1] = strdup(actualabspath);
            ++nDir;
        }
    }

    dupfnd_t *pack = NULL;
    FILE *fout = stdout;
    int flags = FLAG_NONE;

    int nThreads = args.threads;
    if (nThreads < 1 || nThreads > 100)
        nThreads = 1;

    // if (args.progress)      SETFLAG(flags, FLAG_SHOW_PROGRESS);
    if (args.crcmethod)      SETFLAG(flags, args.crcmethod);
    if (args.show_summary)   SETFLAG(flags, FLAG_SHOW_SUMMARY);
    if (args.show_groupnum)  SETFLAG(flags, FLAG_SHOW_GROUPNUM);
    if (args.show_files)     SETFLAG(flags, FLAG_SHOW_FILES);
    if (args.show_size)      SETFLAG(flags, FLAG_SHOW_SIZE);
    if (args.recursive)      SETFLAG(flags, FLAG_RECURSIVE);
    if (args.skipemptyfiles) SETFLAG(flags, FLAG_SKIPEMPTYFILES);

    pack = dupfnd_pack_init(NULL, dirs, nDir, flags, fout, nThreads);

    for (i = 0; i < nDir; ++i)
        free(dirs[i]);
    free(dirs);

    dupfnd_search(pack);
    dupfnd_print_result(pack);
    pack = dupfnd_pack_destory(pack);

    return EXIT_SUCCESS;
}