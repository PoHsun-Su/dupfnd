#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include "util.h"

const char *get_filename_ext(const char *filename) {
    const char *dot = strrchr(filename, '.');
    if(!dot || dot == filename)
        return "";
    return dot + 1;
}

inline int isNameExcluded(const char *pName) {
    if(0 && likely(pName)) {
        if (0 == strcmp(".", pName)) {
            return 1;
        }
        if ('.' == pName[0]) {
            return 1;
        }
    }
    return 0;
}

inline int isDirExcluded(const char *pName, const struct stat64 *pStatbuf) {
    if (0 && likely(pStatbuf && S_ISDIR(pStatbuf->st_mode))) {
        if (likely(pName)) {
            if (0 == strcmp("CMakeFiles", pName))
                return 1;
            if (0 == strcmp(".git", pName))
                return 1;
        }
    }
    return 0;
}

inline int isFileExcluded(const char *pName, const struct stat64 *pStatbuf) {
    if (0 && likely(pStatbuf && S_ISREG(pStatbuf->st_mode))) {
        if (likely(pName)) {
            if (0 == strcmp("o", get_filename_ext(pName))) {
                return 1;
            }
        }
    }
    return 0;
}

inline const char *pathJoin(char *dirname, char *basename) {
    char *p = (char *)malloc(strlen(dirname) + strlen(SEPARATOR_STR) + strlen(basename) + 1);
    strcpy(p, dirname);
    strcat(p, SEPARATOR_STR);
    strcat(p, basename);
    return p;
}

inline char *pathJoinReplace(char **pDirname, char *basename) {
    char *p = *pDirname;
    *pDirname = (char *)realloc(p, strlen(p) + strlen(SEPARATOR_STR) + strlen(basename) + 1);
    strcat(p, SEPARATOR_STR);
    strcat(p, basename);
    return p;
}

