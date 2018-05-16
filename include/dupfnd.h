#ifndef __DUPFINDER__
#define __DUPFINDER__

#include <errno.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <zlib.h>

#include "list.h"
#include "rbtree.h"
#include "util.h"

/* === [ constant definitions ] ==================== */
#define BUF_TRUNK_SIZE (1024)
#define LEN_STR_UNIT (1024)

#define WORKQUEUE_TYPE_EMPTY      (0x0)
#define WORKQUEUE_TYPE_TRAVERSE   (0x1)
#define WORKQUEUE_TYPE_COMPARE    (0x2)
#define WORKQUEUE_TYPE_TERMINATE  (0x3)
#define WORKQUEUE_TYPE_CRC        (0x4)

/* === [ flags definitions ] ==================== */
#define FLAG_NONE            (0L)
#define FLAG_SHOW_PROGRESS   (1L << 0)
#define FLAG_SHOW_SUMMARY    (1L << 1)
#define FLAG_SHOW_SIZE       (1L << 2)
#define FLAG_SHOW_FILES      (1L << 3)
#define FLAG_SHOW_GROUPNUM   (1L << 4)
#define FLAG_BYPASS_EXCLUDES (1L << 5)
#define FLAG_BYPASS_HIDDENS  (1L << 6)
#define FLAG_CRC_SW          (1L << 7)
#define FLAG_CRC_SSE         (1L << 8)
#define FLAG_CRC_GPU         (1L << 9)
#define FLAG_RECURSIVE       (1L <<10)
#define FLAG_SKIPEMPTYFILES  (1L <<11)

/* === [ utily macros ] ==================== */
#ifdef __GNUC__
#   ifndef likely
#       define likely(x)       __builtin_expect(!!(x), 1)
#   endif
#   ifndef unlikely
#       define unlikely(x)     __builtin_expect(!!(x), 0)
#   endif
#else
#   ifndef likely
#       define likely(x)       (x)
#   endif
#   ifndef unlikely
#       define unlikely(x)     (x)
#   endif
#endif


#define GETFLAG(f, b) (f & b)
#define SETFLAG(f, b) (f |= b)


#ifndef handle_error_en
#    define handle_error_en(en, msg) \
            do { errno = en; perror(msg); exit(EXIT_FAILURE); } while (0)
#endif
#ifndef handle_error
#    define handle_error(msg) \
             do { perror(msg); exit(EXIT_FAILURE); } while (0)
#endif


/* === [ struct definitions ] ==================== */
typedef struct _filenode {
    char      *pfilepath;       /* Allocted and store full file path , must free before obj destroyed */
    char      *pfilename;       /* Just pointing Do not free this */
    uint32_t  crcpartial;       
    uint32_t  crc;              
    struct list_head llinkblue; /* horizontal, for general use */
    struct list_head llinkred;  /* vertical, for duplicates */

    dev_t     st_dev;           /* ID of device containing file */
    ino64_t   st_ino;           /* Inode number */
    off64_t   st_size;          /* Total size, in bytes */
    // mode_t    st_mode;
    // nlink_t   st_nlink;         /* Number of hard links */
    // uid_t     st_uid;           /* User ID of owner */
    // gid_t     st_gid;           /* Group ID of owner */
    // time_t    st_atim;         /* Time of last access */
    // time_t    st_mtim;         /* Time of last modification */
    // time_t    st_ctim;         /* Time of last status change */
} filenode_t;


typedef struct _groupnode {
    struct list_head llinkblue; /* horizontal, for general use */
    struct list_head llinkred;  /* vertical, for duplicates */
} groupnode_t;



typedef struct _treenode {
    off64_t   st_size;
    struct rb_node   rblink;        
    struct list_head llinkgrouphead;
    struct list_head llinkstorehead;
    pthread_mutex_t mux;
} treenode_t;

typedef struct __pack {
    struct list_head *pllinktrav;
    struct rb_root rbrootlink;
    pthread_mutex_t mux_rbtree;
    char **dirs;
    int nDirs;
    uint32_t flags;
    FILE* stream;
    int nThreads;
} dupfnd_t;

/* workqueue */
typedef struct __workqueue {
    struct list_head linkqueue;
    pthread_mutex_t mux_queue;
    sem_t sem_queue;
    int validate;
} workqueue_t;

typedef struct _queuenode
{
    struct list_head linkqueue;
    int type;
    void *args;
} queuenode_t;

/* traverse */
typedef struct _traveral_args {
    struct list_head *pllinktrav;
    struct rb_root  *prbrootlink;
    pthread_mutex_t *pmux_rbtree;
    int flags;
} traveral_args_t;

/* crc */
typedef struct _crc_args {
    filenode_t *pfilenode;
    uint64_t u64sizelimit;
} crc_args_t;

/* === [ function prototypes ] ==================== */

/* path / unit */
int dupfnd_isfoldercontainedby(const char *lower, const char *upper);
int dupfnd_isfoldercontains(const char *upper, const char *lower);
int dupfnd_isfolderequal(const char *upper, const char *lower);
const char *dupfnd_unitstr(uint64_t size, char *pbuf, uint32_t length, int use10base);

/* workqueue */
queuenode_t *dupfnd_queuenode_init(queuenode_t* qnd, int type, void *args);
queuenode_t *dupfnd_queuenode_destory(queuenode_t* qnd);

workqueue_t *dupfnd_workqueue_init(workqueue_t* wq);
void *dupfnd_workqueue_insert(workqueue_t *workqueue, queuenode_t *qnd);
queuenode_t *dupfnd_workqueue_fetch(workqueue_t* wq);
workqueue_t *dupfnd_workqueue_destroy(workqueue_t* wq);

void *dupfnd_workerthread_func(void *arg);
pthread_t *dupfnd_workerthread_create(pthread_t *workerthreads, uint32_t nthreads, void *(*func)(void *), void *arg);
pthread_t *dupfnd_workerthread_join_destroy(pthread_t *workerthreads, uint32_t nthreads);

/* filenode */
filenode_t *dupfnd_filenode_del(filenode_t *p);
filenode_t *dupfnd_filenode_init(filenode_t *p);

/* treenode */
treenode_t *dupfnd_treenode_init(treenode_t *p, off64_t st_size);
treenode_t *dupfnd_treenode_del(treenode_t *p, struct rb_root *rbrootlink) ;
treenode_t *dupfnd_treenode_insert(struct rb_root *prbtreeroot, treenode_t *ptreenodenew, pthread_mutex_t *pmux_rbtree);

/* groupnode */
groupnode_t *dupfnd_groupnode_init(groupnode_t *p);
groupnode_t *dupfnd_groupnode_del(groupnode_t *p);

/* compare */
#if defined(sse_crc32)
uint32_t dupfnd_ssecrc(uint32_t hash, const void* data, uint32_t bytes);
#endif
uint32_t dupfnd_getfilecrc(char *path, uint64_t u64filesize, uint64_t u64sizelimit);
uint32_t dupfnd_getcrcpartial_impl(crc_args_t *argt);

void *dupfnd_compare_impl(treenode_t *ptreenode, unsigned char *bf1, unsigned char *bf2, uint32_t buflen);
int dupfnd_compare(dupfnd_t *pack);

/* workflow */
dupfnd_t* dupfnd_pack_init(dupfnd_t* pack, char **dirs, int ndirs, uint32_t flags, FILE *stream, int nthreads);

int dupfnd_search(dupfnd_t *pack);
int dupfnd_pushtarget(dupfnd_t* pack);
void dupfnd_print_result(dupfnd_t *pack);

int dupfnd_traverse(dupfnd_t* pack);
int dupfnd_travel_impl(traveral_args_t *traveral_args);
traveral_args_t *dupfnd_traveral_args_init(
        traveral_args_t* ta, struct list_head *pllinktrav,
        struct rb_root  *prbrootlink, pthread_mutex_t *pmux_rbtree, int flags);
traveral_args_t *dupfnd_traveral_args_destory(traveral_args_t* ta);

int dupfnd_trim_size(dupfnd_t* pack);

int dupfnd_all_crc(dupfnd_t *pack);


int dupfnd_findmatches(filenode_t *p1, filenode_t *p2, unsigned char* buf1, unsigned char* buf2, uint32_t lenbuf);

void *dupfnd_trim_uniq_impl(treenode_t* ptreenode);
int dupfnd_trim_uniq(dupfnd_t* pack);

void dupfnd_tree_clean(struct rb_root *rbrootlink);
void dupfnd_tree_print(struct rb_root *rbrootlink, int flags);

dupfnd_t *dupfnd_pack_destory(dupfnd_t *pack);

#endif