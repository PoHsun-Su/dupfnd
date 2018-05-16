#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <nmmintrin.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdint.h>
#include <stdio.h>         /* printf, scanf, puts */
#include <stdlib.h>        /* realloc, free, exit, NULL */
#include <string.h>
#include <sys/types.h>
#include <zlib.h>

#include "list.h"
#include "rbtree.h"
#include "util.h"
#include "dupfnd.h"

#if defined(SSE_CRC32)
inline uint32_t dupfnd_ssecrc(uint32_t hash, const void* data, uint32_t bytes){
    uint32_t words = bytes / sizeof(uint32_t);
    bytes = bytes % sizeof(uint32_t);

    const uint32_t* p = (const uint32_t*)(data);
    while (words--) {
      hash = _mm_crc32_u32(hash, *p);
      ++p;
    }

    const uint8_t* s = (const uint8_t*)(p);
    while (bytes--) {
      hash = _mm_crc32_u8(hash, *s);
      ++s;
    }

    return hash ^ 0xFFFFFFFF;
}
#endif

inline uint32_t dupfnd_getfilecrc(char *path, uint64_t u64filesize, uint64_t u64sizelimit) {
    uint32_t valcrc32 = 0x0;
    FILE *fp = NULL;
    long lbytesremain = u64filesize;
    long lbytestoread;
    unsigned char *pbuffer;
    size_t lengthResult;

    if (u64sizelimit > 0 && u64sizelimit != u64filesize) {
        if (lbytesremain > u64sizelimit) {
            lbytesremain = u64sizelimit;
        }
    }

    #ifdef SSE_CRC32
    valcrc32 = 0xFFFFFFFF;
    #else
    valcrc32 = crc32(0L, Z_NULL, 0);
    #endif

    // allocate memory to contain the whole file:
    pbuffer = (unsigned char*) malloc (sizeof(char)* BUF_TRUNK_SIZE);
    if (pbuffer == NULL) {
        fputs ("Memory error",stderr);
        exit (2);
    }

    if (NULL != (fp = fopen(path, "rb"))) {
        lbytestoread = unlikely(lbytesremain < BUF_TRUNK_SIZE) ? lbytesremain : BUF_TRUNK_SIZE;

        while (likely(lbytestoread > 0)) {
            lengthResult = fread(pbuffer, lbytestoread, 1, fp);
            if (unlikely(lengthResult != 1)) {
                free(pbuffer);
                fclose(fp);
                fputs ("Reading error 1",stderr);
                fprintf(stderr, "path = %s, lbytestoread = %lu, u64filesize =%lu, lengthResult = %lu\n", path, lbytestoread, u64filesize, lengthResult);
                exit (-1);
            }

            #ifdef SSE_CRC32
            valcrc32 = dupfnd_ssecrc(valcrc32, pbuffer, lbytestoread);
            #else
            valcrc32 = crc32(valcrc32, pbuffer, lbytestoread);
            #endif

            if (unlikely(lbytesremain < BUF_TRUNK_SIZE)) {
                /* already read the last trunk at the code above */
                break;
            } else {
                lbytesremain -= BUF_TRUNK_SIZE;
                if (unlikely((lbytesremain < BUF_TRUNK_SIZE)))
                    lbytestoread = lbytesremain; 
            }
        }
        fclose(fp);
        if (pbuffer) {
            free(pbuffer);
        }
    }
    return valcrc32;
}

int dupfnd_isfoldercontainedby(const char *lower, const char *upper) {
    int nUpper = strlen(upper);
    int nLower = strlen(lower);
    if (upper[nUpper - 1] == SEPARATOR)
        --nUpper;
    if (lower[nLower - 1] == SEPARATOR)
        --nLower;

    if (nUpper < nLower)
        if (lower[nUpper] == SEPARATOR && !strncmp(upper, lower, nUpper))
            return 1;

    return 0;
}

int dupfnd_isfoldercontains(const char *upper, const char *lower) {
    return dupfnd_isfoldercontainedby(lower, upper);
}

int dupfnd_isfolderequal(const char *upper, const char *lower) {
    int nUpper = strlen(upper);
    int nLower = strlen(lower);
    if (upper[nUpper - 1] == SEPARATOR)
        --nUpper;
    if (lower[nLower - 1] == SEPARATOR)
        --nLower;
    if (nUpper == nLower)
        if (!strncmp(upper, lower, nUpper<nLower?nUpper:nLower))
            return 1;

    return 0;
}

inline const char *dupfnd_unitstr(uint64_t size, char *pbuf, uint32_t length, int use10base) {
    if (!use10base) {
        if (size < (1UL<<10)) {
            snprintf(pbuf, length, "%3ld Byte", size);
        } else if (size <= (1UL<<20)) {
            snprintf(pbuf, length, "%6.3lf KB", size * 1.0f / (1UL<<10));
        } else if (size <= (1UL<<30)) {
            snprintf(pbuf, length, "%6.3lf MB", size * 1.0f / (1UL<<20));
        } else {
            snprintf(pbuf, length, "%6.3lf GB", size * 1.0f / (1UL<<30));
        }
    } else {
        if (size < (1000L)) {
            snprintf(pbuf, length, "%.0f bytes", size * 1.0f);
        } else if (size <= (1000L * 1000L)) {
            snprintf(pbuf, length, "%.1f kilobytes", size * 1.0f / (1000L));
        } else if (size <= (1000L * 1000L * 1000L)) {
            snprintf(pbuf, length, "%.1f megabytes", size * 1.0f / (1000L * 1000L));
        } else {
            snprintf(pbuf, length, "%.1f gigabytes", size * 1.0f / (1000L * 1000L * 1000L));
        }
    }
    return pbuf;
}

dupfnd_t* dupfnd_pack_init(dupfnd_t* pack, char **dirs, int nDirs, uint32_t flags, FILE* stream, int nThreads) {
    if (!pack)
        pack = (dupfnd_t *)calloc(1, sizeof(dupfnd_t));
    pack->pllinktrav = (struct list_head *)malloc(1 * sizeof(struct list_head));
    INIT_LIST_HEAD(&pack->pllinktrav[0]);
    pthread_mutex_init(&pack->mux_rbtree, NULL);
    pack->rbrootlink = RB_ROOT;
    pack->nDirs = 0;
    pack->flags = flags;
    pack->stream = stream;
    pack->nThreads = nThreads;

    int i = 0;
    int j = 0;
    int addthis = 0;
    /* Eliminate redundant path or subfolder */
    for (i = 0; i<nDirs; ++i) {
        addthis = 1;
        for (j = 0; j<nDirs; ++j) {
            if (i == j)
                continue;
            if (dupfnd_isfolderequal(dirs[i], dirs[j])) {
                if (j < i) {
                    addthis = 0;
                    break;
                } else {
                    continue;
                }
            }
            if (dupfnd_isfoldercontainedby(dirs[i], dirs[j])) {
                addthis = 0;
                break;
            }
        }
        if (addthis) {
            ++(pack->nDirs);
            pack->dirs = (char **)realloc(pack->dirs, pack->nDirs * sizeof(char *));
            pack->dirs[pack->nDirs - 1] = strdup(dirs[i]);
        }
    }
    return pack;
}

filenode_t *dupfnd_filenode_del(filenode_t *p) {
    if (p) {
        list_del(&p->llinkblue);
        list_del(&p->llinkred);
        if (p->pfilepath) {
            free(p->pfilepath);
            p->pfilepath = NULL;

            // pfilename is only a pointer pointing to filename part in pfilepath
            // DO NOT try to free pfilename
            p->pfilename = NULL;
        }
        free(p);
    }
    return NULL;
}

groupnode_t *dupfnd_groupnode_del(groupnode_t *p) {
    if (p) {
        list_del(&p->llinkblue);
        list_del(&p->llinkred);
        free(p);
    }
    return NULL;
}

dupfnd_t *dupfnd_pack_destory(dupfnd_t *pack) {
    if (pack) {
        int i;
        for (i = 0; i < pack->nDirs ; ++i) {
            free(pack->dirs[i]);
        }
        free(pack->dirs);
        pthread_mutex_destroy(&pack->mux_rbtree);

        filenode_t *pfilenode;
        filenode_t *saferef;
        for (i=0; i<pack->nDirs; ++i) {
            list_for_each_entry_safe(pfilenode, saferef, &pack->pllinktrav[i], llinkblue) {
                if (pfilenode)
                    dupfnd_filenode_del(pfilenode);
            }
        }
        free(pack->pllinktrav);

        dupfnd_tree_clean(&pack->rbrootlink);
        free(pack);
    }
    return NULL;
}

filenode_t *dupfnd_filenode_init(filenode_t *p) {
    if (!p)
        p = (filenode_t *)malloc(sizeof(filenode_t));
    p->pfilepath = NULL;
    p->pfilename = NULL;
    p->crcpartial = 0;
    p->crc = 0;
    INIT_LIST_HEAD(&p->llinkblue);
    INIT_LIST_HEAD(&p->llinkred);
    return p;
}

groupnode_t *dupfnd_groupnode_init(groupnode_t *p) {
    if (!p)
        p = (groupnode_t *)malloc(sizeof(groupnode_t));
    INIT_LIST_HEAD(&p->llinkblue);
    INIT_LIST_HEAD(&p->llinkred);
    return p;
}

treenode_t *dupfnd_treenode_init(treenode_t *p, off64_t st_size) {
    if (!p) {
        p = (treenode_t *)malloc(sizeof(treenode_t));
        INIT_LIST_HEAD(&p->llinkgrouphead);
        INIT_LIST_HEAD(&p->llinkstorehead);
        pthread_mutex_init(&p->mux, NULL);
    }
    p->st_size = st_size;
    return p;
}

treenode_t *dupfnd_treenode_del(treenode_t *p, struct rb_root *rbrootlink) {
    if (p) {
        filenode_t *pfilenode;
        filenode_t *safefilenoderef;
        groupnode_t *pgroupnode;
        groupnode_t *safegroupref;
        pthread_mutex_lock(&p->mux);
        list_for_each_entry_safe(pgroupnode, safegroupref, &p->llinkgrouphead, llinkblue) {
            if (pgroupnode) {
                list_for_each_entry_safe(pfilenode, safefilenoderef, &pgroupnode->llinkred, llinkred) {
                    dupfnd_filenode_del(pfilenode);
                }
                dupfnd_groupnode_del(pgroupnode);
            }
        }

        list_for_each_entry_safe(pfilenode, safefilenoderef, &p->llinkstorehead, llinkred) {
            dupfnd_filenode_del(pfilenode);
        }

        rb_erase(&p->rblink, rbrootlink);
        pthread_mutex_unlock(&p->mux);
        pthread_mutex_destroy(&p->mux);
        free(p);
    }
    return NULL;
}

void dupfnd_tree_clean(struct rb_root *rbrootlink) {
    struct rb_node *rb_iter = NULL;
    treenode_t *pTreeNode = NULL;

    int clean_done = 0;
    while (!clean_done) {
        clean_done = 1;
        for (rb_iter = rb_first(rbrootlink); rb_iter; rb_iter = rb_next(rb_iter)) {
            pTreeNode = rb_entry(rb_iter, treenode_t, rblink);
            if (likely(pTreeNode)) {
                pTreeNode = dupfnd_treenode_del(pTreeNode, rbrootlink);
                clean_done = 0;
                break;
            }
        }
    }
}

int dupfnd_pushtarget(dupfnd_t* pack) {
    filenode_t *ptargets;
    pack->pllinktrav = (struct list_head *)realloc(pack->pllinktrav, pack->nDirs * sizeof(struct list_head));
    int i = 0;
    for (i = 0; i< pack->nDirs; ++i) {
        INIT_LIST_HEAD(&pack->pllinktrav[i]);
        ptargets = dupfnd_filenode_init(NULL);
        ptargets->pfilepath = strdup(pack->dirs[i]);
        ptargets->pfilename = strrchr(ptargets->pfilepath, SEPARATOR);
        /* No needed to free ptargets since we free it when it pops */
        /* one or more initial search directories */
        list_add_tail(&ptargets->llinkblue, &pack->pllinktrav[i]);
    }
    return i;
}

treenode_t *dupfnd_treenode_insert(struct rb_root *pRbTreeRoot, treenode_t *pTreeNodeNew, pthread_mutex_t *pmux_rbtree) {
    struct rb_node **pNewRbNode = &(pRbTreeRoot->rb_node), *parent = NULL;
    int64_t result = 0;
    treenode_t *this = NULL;
    /* Figure out where to put pNewRbNode node */
    pthread_mutex_lock(pmux_rbtree);
    while (likely(*pNewRbNode)) {
        this = rb_entry(*pNewRbNode, treenode_t, rblink);
        result = pTreeNodeNew->st_size < this->st_size ? -1 : (pTreeNodeNew->st_size > this->st_size ? 1 : 0);
        parent = *pNewRbNode;
        if (result < 0)
            pNewRbNode = &((*pNewRbNode)->rb_left);
        else if (result > 0)
            pNewRbNode = &((*pNewRbNode)->rb_right);
        else {
            /* Already exists a node with the same key, not going to insert */
            pthread_mutex_unlock(pmux_rbtree);
            return this;
        }
    }

    /* Add new node and rebalance tree. */
    rb_link_node(&pTreeNodeNew->rblink, parent, pNewRbNode);
    rb_insert_color(&pTreeNodeNew->rblink, pRbTreeRoot);

    pthread_mutex_unlock(pmux_rbtree);
    return pTreeNodeNew;
}

int dupfnd_travel_impl(traveral_args_t *traveral_args) {
    if (!traveral_args)
        return 0;
    struct list_head *pllinktrav  = traveral_args->pllinktrav;
    pthread_mutex_t *pmux_rbtree  = traveral_args->pmux_rbtree;
    struct rb_root  *prbrootlink  = traveral_args->prbrootlink;

    DIR *dirp;
    struct dirent *entry;
    struct stat64 statbuf;

    char *pCurFullPath = NULL;
    char *pCurBaseName = NULL;

    filenode_t *pQueueTop = NULL;
    treenode_t *pTreeNodeToMigrate = NULL;
    treenode_t *pTreeNodeTempBuf = NULL;

    struct list_head llinkfiles;
    INIT_LIST_HEAD(&llinkfiles);

    while (!list_empty(pllinktrav)) {
        pQueueTop = list_first_entry(pllinktrav, typeof(filenode_t), llinkblue);
        if (unlikely(!pQueueTop)) {
            break;
        }
        dirp = opendir(pQueueTop->pfilepath);
        if (unlikely(!dirp)) {
            if (ENOENT == errno) {
                fprintf(stderr,"no such directory: %s\n", pQueueTop->pfilepath);
            } else {
                fprintf(stderr,"cannot open directory: %s\n", pQueueTop->pfilepath);
            }
            dupfnd_filenode_del(pQueueTop);
            continue;
        }

        while(likely((entry = readdir(dirp)) != NULL)) {
            /* ignore . and .. */
            if(strcmp(".",entry->d_name) == 0 ||
                strcmp("..",entry->d_name) == 0)
                continue;

    #if defined(EXCLUDE_RULES)
            if (isNameExcluded(entry->d_name))
                continue;
    #endif
            /* Create FullPath for this item */
            /* pCurFullPath Must be NULL or and address previously allocated by malloc(), calloc() or realloc() ! */
            
           // 'dirname' '/' 'basename' '\0'
            pCurFullPath = (char *)realloc(pCurFullPath, sizeof(char) * (
                strlen(pQueueTop->pfilepath)    /* dirname  */
                + strlen("/")                        /* /        */
                + strlen(entry->d_name)              /* basename */
                + 1)                                 /* NULL     */
            );
            strcpy(pCurFullPath, pQueueTop->pfilepath);
            strcat(pCurFullPath, SEPARATOR_STR);
            pCurBaseName = pCurFullPath + (strlen(pCurFullPath));
            strcat(pCurFullPath, entry->d_name);

            /* Get File Stat */
            fstatat64(0, pCurFullPath, &statbuf, AT_SYMLINK_NOFOLLOW);
            if(S_ISDIR(statbuf.st_mode) && GETFLAG(traveral_args->flags, FLAG_RECURSIVE)) {

    #if defined(EXCLUDE_RULES)
                if (isDirExcluded(entry->d_name, &statbuf))
                    continue;
    #endif
                /* Push if it's a folder */
                filenode_t *pListNodeDir = dupfnd_filenode_init(NULL);

                pListNodeDir->pfilepath = pCurFullPath;
                pListNodeDir->pfilename = pCurBaseName;
                /* Marked this memory are as taken, so create another one on next turn */
                pCurFullPath = NULL;

                list_add(&pListNodeDir->llinkblue, &pQueueTop->llinkblue);
            } else if(S_ISREG(statbuf.st_mode)) {
                if (0 == statbuf.st_size && GETFLAG(traveral_args->flags, FLAG_SKIPEMPTYFILES))
                    continue;

                /* Added to tree if it's a file */
    #if defined(EXCLUDE_RULES)
                if (isFileExcluded(entry->d_name, &statbuf))
                    continue;
    #endif
                /* 0. Always prepare a tree node to search and insert */
                /* 1. Update key, search where to put this item, Create and insert one if needed  */
                pTreeNodeTempBuf = dupfnd_treenode_init(pTreeNodeTempBuf, statbuf.st_size);
                pTreeNodeToMigrate = dupfnd_treenode_insert(prbrootlink, pTreeNodeTempBuf, pmux_rbtree);

                /* 2. the key is new to the tree, so we inserted into the tree successfully */
                if (unlikely(pTreeNodeToMigrate == pTreeNodeTempBuf)) {
                    /*  set to NULL to Mark this node is occupied,
                    so that a new tree node pTreeNodeTempBuf will be created at next run */
                    pTreeNodeTempBuf = NULL;
                }

                /* 3. Create a list node */
                filenode_t *pFileNode   = dupfnd_filenode_init(NULL);
                pFileNode->pfilepath    = pCurFullPath;
                // printf("%s\n", pCurFullPath);
                pFileNode->pfilename    = pCurBaseName;
                pFileNode->st_size      = statbuf.st_size;
                pFileNode->st_dev       = statbuf.st_dev;
                pFileNode->st_ino       = statbuf.st_ino;
                // pFileNode->st_mode      = statbuf.st_mode;
                // pFileNode->st_nlink     = statbuf.st_nlink;
                // pFileNode->st_uid       = statbuf.st_uid;
                // pFileNode->st_gid       = statbuf.st_gid;
                // pFileNode->st_atim      = statbuf.st_atim;
                // pFileNode->st_mtim      = statbuf.st_mtim;
                // pFileNode->st_ctim      = statbuf.st_ctim;

                /* Marked this memory are as taken, so create another one on next turn */
                pCurFullPath = NULL;

                /* 3. Added the list node at the back of the list of the found tree */
                list_add_tail(&pFileNode->llinkblue, &llinkfiles);


                /* 4. temporarily store this filenode in the storage space of the treenode */
                pthread_mutex_lock(&pTreeNodeToMigrate->mux);
                list_add_tail(&pFileNode->llinkred, &pTreeNodeToMigrate->llinkstorehead);
                pthread_mutex_unlock(&pTreeNodeToMigrate->mux);
            } /* S_ISDIR */
        } /* while readdir */
        closedir(dirp);

        /* pop and free top nodes */
        dupfnd_filenode_del(pQueueTop);
    }

    if (unlikely(pCurFullPath)) {
        free(pCurFullPath);
        pCurFullPath = NULL;
    }
    if (unlikely(pTreeNodeTempBuf)) {
        free(pTreeNodeTempBuf);
        pTreeNodeTempBuf = NULL;
    }

    assert(pllinktrav->next == pllinktrav);
    assert(pllinktrav->prev == pllinktrav);
    list_replace(&llinkfiles, pllinktrav);
}

uint32_t dupfnd_getcrcpartial_impl(crc_args_t *argt) {
    return dupfnd_getfilecrc(argt->pfilenode->pfilepath, argt->pfilenode->st_size, argt->u64sizelimit);
}

workqueue_t *dupfnd_workqueue_init(workqueue_t* wq) {
    if (!wq)
        wq = (workqueue_t *)calloc(1, sizeof(workqueue_t));

    INIT_LIST_HEAD(&wq->linkqueue);
    pthread_mutex_init(&wq->mux_queue, NULL);
    if (sem_init(&wq->sem_queue, 0, 0) == -1)
        handle_error("sem_init");
    wq->validate = 0xabcd;
    return wq;
}

workqueue_t *dupfnd_workqueue_destroy(workqueue_t* wq) {
    if (!wq) return NULL;
    queuenode_t *qnd = NULL;
    queuenode_t *qndSafeRef = NULL;
    pthread_mutex_lock(&wq->mux_queue);
    if (!list_empty(&wq->linkqueue))
        list_for_each_entry_safe(qnd, qndSafeRef, &wq->linkqueue, linkqueue)
            dupfnd_queuenode_destory(qnd);

    pthread_mutex_unlock(&wq->mux_queue);
    pthread_mutex_destroy(&wq->mux_queue);
    sem_destroy(&wq->sem_queue);
    free(wq);
    return NULL;
}

queuenode_t *dupfnd_queuenode_init(queuenode_t* qnd, int type, void *args) {
    if (!qnd) qnd = (queuenode_t *)calloc(1, sizeof(queuenode_t));
    INIT_LIST_HEAD(&qnd->linkqueue);
    qnd->type = type;
    qnd->args = args;
    return qnd;
}

queuenode_t *dupfnd_workqueue_fetch(workqueue_t* wq) {
    if (!wq) return NULL;
    queuenode_t *qnd = NULL;
    sem_wait(&wq->sem_queue);
    pthread_mutex_lock(&wq->mux_queue);
    if (!list_empty(&wq->linkqueue)) {
        qnd = list_first_entry(&wq->linkqueue, queuenode_t, linkqueue);
        list_del_init(&qnd->linkqueue);
    }
    pthread_mutex_unlock(&wq->mux_queue);
    return qnd;
}

queuenode_t *dupfnd_queuenode_destory(queuenode_t* qnd) {
    if (!qnd) return NULL;

    switch (qnd->type) {
        case WORKQUEUE_TYPE_TRAVERSE:
            {
                traveral_args_t *argt = (traveral_args_t *)qnd->args;
                argt = dupfnd_traveral_args_destory(argt);
            }
            break;
        case WORKQUEUE_TYPE_EMPTY:
            break;
        case WORKQUEUE_TYPE_COMPARE:
            break;
        case WORKQUEUE_TYPE_TERMINATE:
            break;
        case WORKQUEUE_TYPE_CRC:
            {
                crc_args_t *argt = (crc_args_t *)qnd->args;
                free(argt);
                argt = NULL;
            }
            break;
        default:
            assert(0);
            break;
    }
    free(qnd);
    return NULL;
}

traveral_args_t *dupfnd_traveral_args_init(
        traveral_args_t* ta, struct list_head *pllinktrav,
        struct rb_root  *prbrootlink, pthread_mutex_t *pmux_rbtree, int flags)
{
    if (!ta) ta = (traveral_args_t *)calloc(1, sizeof(traveral_args_t));

    ta->pllinktrav = pllinktrav;
    ta->prbrootlink = prbrootlink;
    ta->pmux_rbtree = pmux_rbtree;
    ta->flags = flags;
    return ta;
}

traveral_args_t *dupfnd_traveral_args_destory(traveral_args_t* ta) {
    if (ta) {
        free(ta);
        ta = NULL;
    }
    return NULL;
}

void *dupfnd_workqueue_insert(workqueue_t *workqueue, queuenode_t *qnd) {
    pthread_mutex_lock(&workqueue->mux_queue);
    list_add_tail(&qnd->linkqueue, &workqueue->linkqueue);
    pthread_mutex_unlock(&workqueue->mux_queue);
    sem_post(&workqueue->sem_queue);
    return NULL;
}

void *dupfnd_workerthread_func(void *arg) {
    if (!arg) return NULL;
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);

    workqueue_t *wq = (workqueue_t *)arg;
    queuenode_t *qnd = NULL;
    if (wq->validate != 0xabcd) {
        printf("workqueue version mismatched \n");
        return NULL;
    }
    int running = 1;

    unsigned char *bf1 = NULL;
    unsigned char *bf2 = NULL;
    while (running) {
        if ((qnd = dupfnd_workqueue_fetch(wq))) {
            switch (qnd->type) {
                case WORKQUEUE_TYPE_EMPTY:
                    break;
                case WORKQUEUE_TYPE_TRAVERSE:
                    {
                        traveral_args_t *argt = (traveral_args_t *)qnd->args;
                        dupfnd_travel_impl(argt);
                    }
                    break;
                case WORKQUEUE_TYPE_COMPARE:
                    {
                        void *ret = NULL;
                        uint32_t buflen = 8192;
                        if (!bf1)
                            if (!(bf1 = (unsigned char *)realloc(bf1, sizeof(char) * buflen))) {
                                running = 0;
                                break;
                            }

                        if (!bf2)
                            if (!(bf2 = (unsigned char *)realloc(bf2, sizeof(char) * buflen))) {
                                free(bf1);
                                bf1 = NULL;
                                running = 0;
                                break;           
                            }

                        treenode_t *pTreeNode = (treenode_t *)qnd->args;
                        dupfnd_compare_impl(pTreeNode, bf1, bf2, buflen);
                    }
                    break;
                case WORKQUEUE_TYPE_CRC:
                    {
                        crc_args_t *argt = (crc_args_t *)qnd->args;
                        argt->pfilenode->crcpartial = dupfnd_getcrcpartial_impl(argt);
                    }
                    break;
                case WORKQUEUE_TYPE_TERMINATE:
                    {
                        running = 0;
                    }
                    break;
                default:
                    assert(0);
                    break;
            }
            dupfnd_queuenode_destory(qnd);
        }
    }

    if (bf1)
        free(bf1);
    if (bf2)
        free(bf2);
    return NULL;
}

pthread_t *dupfnd_workerthread_create(pthread_t *workerthreads,
    uint32_t nthreads, void *(*func)(void *), void *arg)
{
    if (!workerthreads)
        workerthreads = (pthread_t *)calloc(nthreads, sizeof(pthread_t));
    int i;
    for (i=0; i < nthreads; ++i) {
        if(pthread_create(&workerthreads[i], NULL, func, arg)) {
            fprintf(stderr, "Error creating thread\n");
            int j;
            for (j=0; j<i; ++j)
                pthread_cancel(workerthreads[j]);
            free(workerthreads);
            workerthreads = NULL;
            break;
        }
    }
    return workerthreads;
}

pthread_t *dupfnd_workerthread_join_destroy(pthread_t *workerthreads, uint32_t nthreads) {
    assert(workerthreads);
    int i;
    if (workerthreads) {
        for (i=0; i < nthreads; ++i)
            pthread_join(workerthreads[i], NULL);
        free(workerthreads);
    }
    return NULL;
}

int dupfnd_traverse(dupfnd_t* pack) {
    assert(pack->pllinktrav);

    int i;
    #if defined(DUPFND_THREAD)
        workqueue_t *wq = dupfnd_workqueue_init(NULL);
        pthread_t *workerthreads = dupfnd_workerthread_create(NULL, pack->nThreads, dupfnd_workerthread_func, (void *)wq);

        for (i = 0; i< pack->nDirs; ++i) {
            traveral_args_t *argt = dupfnd_traveral_args_init(NULL, &pack->pllinktrav[i], &pack->rbrootlink, &pack->mux_rbtree, pack->flags);
            queuenode_t *qnd = dupfnd_queuenode_init(NULL, WORKQUEUE_TYPE_TRAVERSE, (void *)argt);
            dupfnd_workqueue_insert(wq, qnd);
        }

        for (i = 0; i< pack->nThreads; ++i) {
            queuenode_t *qnd = dupfnd_queuenode_init(NULL, WORKQUEUE_TYPE_TERMINATE, NULL);
            dupfnd_workqueue_insert(wq, qnd);
        }

        workerthreads = dupfnd_workerthread_join_destroy(workerthreads, pack->nThreads);
        wq = dupfnd_workqueue_destroy(wq);
    #else
        traveral_args_t *argt = NULL;
        for (i = 0; i< pack->nDirs; ++i) {
            argt = dupfnd_traveral_args_init(argt, &pack->pllinktrav[i], &pack->rbrootlink, &pack->mux_rbtree, pack->flags);
            dupfnd_travel_impl(argt);
        }
        argt = dupfnd_traveral_args_destory(argt);
    #endif
    return 0;
}

int dupfnd_findmatches(filenode_t *p1, filenode_t *p2, unsigned char* buf1, unsigned char* buf2, uint32_t lenbuf) {
    if (p1->st_size != p2->st_size)
        return 0;

    if (0 == p1->st_size)
        return 1;

    if (0 == p1->crcpartial)
        p1->crcpartial = dupfnd_getfilecrc(p1->pfilepath, p1->st_size, 4096);

    if (0 == p2->crcpartial)
        p2->crcpartial = dupfnd_getfilecrc(p2->pfilepath, p2->st_size, 4096);

    if (p1->crcpartial != p2->crcpartial)
        return 0;

    if (0 == p1->crc)
        p1->crc = dupfnd_getfilecrc(p1->pfilepath, p1->st_size, 0);

    if (0 == p2->crc)
        p2->crc = dupfnd_getfilecrc(p2->pfilepath, p2->st_size, 0);

    if (p1->crc != p2->crc)
        return 0;

    FILE *fp1 = NULL;
    FILE *fp2 = NULL;
    long lbytesremain = 0;
    long lbytestoread = 0;
    int match = 1;

    fp1 = fopen(p1->pfilepath, "rb");
    if (!fp1) {
        fprintf(stderr, "err fp1, %s\n", p1->pfilepath);
        fflush(stderr);
        return 0;
    }

    fp2 = fopen(p2->pfilepath, "rb");
    if (!fp2) {
        fprintf(stderr, "err fp2 %s\n", p2->pfilepath);
        fflush(stderr);
        return 0;
    }

    lbytesremain = p1->st_size;
    lbytestoread = unlikely(lbytesremain < lenbuf) ? lbytesremain : lenbuf;

    fseek(fp1, 0, SEEK_SET);
    fseek(fp2, 0, SEEK_SET);
    match = 1;

    while (likely(lbytestoread > 0)) {
        if ( 1 != fread(buf1, sizeof(unsigned char) * lbytestoread, 1, fp1)) {
            match = 0;
            break;
        }
        if (1 != fread(buf2, sizeof(unsigned char) * lbytestoread, 1,  fp2)) {
            match = 0;
            break;
        }
        if (0 != memcmp(buf1, buf2, lbytestoread)) {
            match = 0;
            break;
        }

        if (lbytesremain >= lenbuf)
            lbytesremain -= lenbuf;
        else
            break;

        if (unlikely((lbytesremain < lenbuf)))
            lbytestoread = lbytesremain;
    }

    if (likely(fp1))
        fclose(fp1);

    if (likely(fp2))
        fclose(fp2);
    return match;
}

void *dupfnd_compare_impl(treenode_t *pTreeNode, unsigned char *bf1, unsigned char *bf2, uint32_t buflen) {
    /* The tree node should has only one (the default) group at this moment */
    if (list_empty(&pTreeNode->llinkstorehead))
        return NULL;

    if (list_is_singular(&pTreeNode->llinkstorehead)) {
        dupfnd_filenode_del(list_first_entry(&pTreeNode->llinkstorehead, filenode_t, llinkred));
        return NULL;
    }

    int foundMatch = 0;
    filenode_t *dupFileAgent = NULL;
    groupnode_t *pgroupnode = NULL;
    filenode_t *pFileNode;
    filenode_t *pSafeRef;

    list_for_each_entry_safe(pFileNode, pSafeRef, &pTreeNode->llinkstorehead, llinkred) {
        foundMatch = 0;
        /* search element 1 to n for duplicates */
        list_for_each_entry(pgroupnode, &pTreeNode->llinkgrouphead, llinkblue) {
            dupFileAgent = list_first_entry(&pgroupnode->llinkred, filenode_t, llinkred);
            foundMatch = dupfnd_findmatches(pFileNode, dupFileAgent, bf1, bf2, buflen);
            if (foundMatch) {
                list_move_tail(&pFileNode->llinkred, &pgroupnode->llinkred);
                break;
            }
        }

        if (foundMatch) {
            continue;
        } else {
            groupnode_t *newgroup = dupfnd_groupnode_init(NULL);
            list_add_tail(&newgroup->llinkblue, &pTreeNode->llinkgrouphead);
            list_move_tail(&pFileNode->llinkred, &newgroup->llinkred);
        }
    }

    return NULL;
}

int dupfnd_compare(dupfnd_t *pack) {
    struct rb_node *rb_iter = NULL;
    treenode_t *pTreeNode = NULL;
    #if defined(DUPFND_THREAD)
        int i = 0;
        workqueue_t *wq = dupfnd_workqueue_init(NULL);
        pthread_t *workerthreads = dupfnd_workerthread_create(NULL, pack->nThreads, dupfnd_workerthread_func, (void *)wq);

        for (rb_iter = rb_first(&pack->rbrootlink); rb_iter; rb_iter = rb_next(rb_iter)) {
            pTreeNode = rb_entry(rb_iter, treenode_t, rblink);
            queuenode_t *qnd = dupfnd_queuenode_init(NULL, WORKQUEUE_TYPE_COMPARE, (void *)pTreeNode);
            dupfnd_workqueue_insert(wq, qnd);
        }

        for (i = 0; i< pack->nThreads; ++i) {
            queuenode_t *qnd = dupfnd_queuenode_init(NULL, WORKQUEUE_TYPE_TERMINATE, NULL);
            dupfnd_workqueue_insert(wq, qnd);
        }

        workerthreads = dupfnd_workerthread_join_destroy(workerthreads, pack->nThreads);
        wq = dupfnd_workqueue_destroy(wq);
    #else
        unsigned char *bf1 = NULL;
        unsigned char *bf2 = NULL;
        uint32_t buflen = 8192;

        if (!(bf1 = (unsigned char *)malloc(sizeof(char) * buflen))) {
            return NULL;
        }
        if (!(bf2 = (unsigned char *)malloc(sizeof(char) * buflen))) {
            free(bf1);
            bf1 = NULL;
            return NULL;
        }

        for (rb_iter = rb_first(&pack->rbrootlink); rb_iter; rb_iter = rb_next(rb_iter)) {
            pTreeNode = rb_entry(rb_iter, treenode_t, rblink);
            dupfnd_compare_impl(pTreeNode, bf1, bf2, buflen);
        }

        free(bf1);
        free(bf2);

    #endif
    return 0;
}

void *dupfnd_trim_uniq_impl(treenode_t* pTreeNode) {
    groupnode_t *pgroupnode = NULL;
    groupnode_t *safegroupref = NULL;

    /* The tree node should has at least one group (the default group and other duplicate group) at this moment */
    if (list_empty(&pTreeNode->llinkgrouphead))
        return NULL;

    /* Delete all group has none or has only one element */
    list_for_each_entry_safe(pgroupnode, safegroupref, &pTreeNode->llinkgrouphead, llinkblue) {
        if (list_is_singular(&pgroupnode->llinkred))
            dupfnd_filenode_del(list_first_entry(&pgroupnode->llinkred, filenode_t, llinkred));
        if (list_empty(&pgroupnode->llinkred))
            dupfnd_groupnode_del(pgroupnode);
    }

    return NULL;
}

int dupfnd_trim_uniq(dupfnd_t* pack) {
    struct rb_node *rb_iter = NULL;
    treenode_t *pTreeNode = NULL;
    for (rb_iter = rb_first(&pack->rbrootlink); rb_iter; rb_iter = rb_next(rb_iter)) {
        pTreeNode = rb_entry(rb_iter, treenode_t, rblink);
        dupfnd_trim_uniq_impl(pTreeNode);
    }
    return 0;
}

int dupfnd_trim_size(dupfnd_t* pack) {
    struct rb_node *rb_iter = NULL;
    treenode_t *pTreeNode = NULL;
    for (rb_iter = rb_first(&pack->rbrootlink); rb_iter; rb_iter = rb_next(rb_iter)) {
        pTreeNode = rb_entry(rb_iter, treenode_t, rblink);
        if (list_is_singular(&pTreeNode->llinkstorehead))
            dupfnd_filenode_del(list_first_entry(&pTreeNode->llinkstorehead, filenode_t, llinkred));
    }
    return 0;
}

#define COLOR_RED "\033[31m"
#define COLOR_GREEN "\033[32m"
#define COLOR_YELLOW "\033[33m"
#define COLOR_NONE "\033[0m"

void dupfnd_tree_print(struct rb_root *rbrootlink, int flags) {
    struct rb_node *rb_iter = NULL;
    treenode_t *ptreenode = NULL;
    filenode_t *pfilenode = NULL;
    groupnode_t *pgroupnode = NULL;

    uint64_t ndupegroups = 0;
    uint64_t ndupefiles_total = 0;
    uint64_t ndupefiles = 0;
    uint64_t ndupesize = 0;
    uint64_t nfilesize = 0;

    const char *colors[] = {COLOR_GREEN, COLOR_YELLOW, COLOR_NONE};

    int show_summary =  GETFLAG(flags, FLAG_SHOW_SUMMARY);
    int show_size =     GETFLAG(flags, FLAG_SHOW_SIZE);
    int show_groupnum = GETFLAG(flags, FLAG_SHOW_GROUPNUM);
    int show_files =    GETFLAG(flags, FLAG_SHOW_FILES);

    char str_unit[LEN_STR_UNIT];
    int use10base = 1;
    int color_idx = 0;
    int group_idx = 0;

    if (!(show_summary || show_files))
        return;

    for (rb_iter = rb_first(rbrootlink); rb_iter; rb_iter = rb_next(rb_iter)) {
        ptreenode = rb_entry(rb_iter, treenode_t, rblink);
        if (list_empty(&ptreenode->llinkgrouphead))
            continue;

        if (show_size) {
            dupfnd_unitstr(ptreenode->st_size, str_unit, LEN_STR_UNIT, use10base);
            printf(COLOR_RED "Size: %s" COLOR_NONE, str_unit);
            if (show_files)
                printf("\n");
        }

        ndupefiles = 0;
        list_for_each_entry(pgroupnode, &ptreenode->llinkgrouphead, llinkblue) {
            nfilesize = 0;
            list_for_each_entry(pfilenode, &pgroupnode->llinkred, llinkred) {
                nfilesize = pfilenode->st_size;
                ndupesize += nfilesize;
                ++ ndupefiles;
                if (show_files) {
                    if (show_groupnum)
                        printf("%sgroup #%d :\t", colors[color_idx], group_idx);
                    printf("%s%s%s\n", colors[color_idx], pfilenode->pfilepath, COLOR_NONE);
                }
            }
            -- ndupefiles;
            color_idx ^= 1;
            ++ ndupegroups;
            ++ group_idx;
            ndupesize -= nfilesize;
        }
        ndupefiles_total += ndupefiles;

        if (show_files)
            printf("\n");
        else if (show_size)
            printf(COLOR_RED ": " COLOR_GREEN "%ld duplicated files\n" COLOR_NONE, ndupefiles);
    }

    if (show_summary && ndupefiles_total) {
        dupfnd_unitstr(ndupesize, str_unit, LEN_STR_UNIT, use10base);
        printf("%lu duplicate files (in %lu sets), occupying %s\n",  ndupefiles_total, ndupegroups, str_unit);
    }
}

int dupfnd_all_crc(dupfnd_t *pack) {
    assert(pack->pllinktrav);

    int i;
    #if defined(DUPFND_THREAD)
        workqueue_t *wq = dupfnd_workqueue_init(NULL);
        pthread_t *workerthreads = dupfnd_workerthread_create(NULL, pack->nThreads, dupfnd_workerthread_func, (void *)wq);

        filenode_t *pfilenode;
        for (i = 0; i< pack->nDirs; ++i) {
            list_for_each_entry(pfilenode, &pack->pllinktrav[i], llinkblue) {
                if (pfilenode->st_size > 0) {
                    crc_args_t *argt = (crc_args_t *)malloc(sizeof(crc_args_t));
                    argt->pfilenode = pfilenode;
                    argt->u64sizelimit = 4096;
                    queuenode_t *qnd = dupfnd_queuenode_init(NULL, WORKQUEUE_TYPE_CRC, (void *)argt);
                    dupfnd_workqueue_insert(wq, qnd);
                }
            }
        }

        for (i = 0; i< pack->nThreads; ++i) {
            queuenode_t *qnd = dupfnd_queuenode_init(NULL, WORKQUEUE_TYPE_TERMINATE, NULL);
            dupfnd_workqueue_insert(wq, qnd);
        }

        workerthreads = dupfnd_workerthread_join_destroy(workerthreads, pack->nThreads);
        wq = dupfnd_workqueue_destroy(wq);
    #else
        crc_args_t *argt = (crc_args_t *)malloc(sizeof(crc_args_t));
        filenode_t *pfilenode = NULL;
        for (i = 0; i< pack->nDirs; ++i) {
            list_for_each_entry(pfilenode, &pack->pllinktrav[i], llinkblue) {
                if (pfilenode->st_size > 0) {
                    argt->pfilenode = pfilenode;
                    argt->u64sizelimit = 4096;
                    pfilenode->crcpartial = dupfnd_getcrcpartial_impl(argt);
                }
            }
        }
        free(argt);
    #endif
    return 0;
}

void dupfnd_print_result(dupfnd_t *pack) {
    dupfnd_tree_print(&pack->rbrootlink, pack->flags);
}

int dupfnd_search(dupfnd_t *pack) {
    int i = 0;
    i = dupfnd_pushtarget(pack);
    if (i < 1) {
        if (pack->stream)
            fprintf(pack->stream, "nothing inside, bye\n");
        return 0;
    }

    dupfnd_traverse(pack);
    dupfnd_trim_size(pack);
    dupfnd_all_crc(pack);
    dupfnd_compare(pack);
    dupfnd_trim_uniq(pack);

    return 0;
}
