#ifndef __UTILIII__
#define __UTILIII__

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

#define SEPARATOR '/'
#define SEPARATOR_STR "/"

const char *get_filename_ext(const char *filename);
const char *pathJoin(char *dirname, char *basename);
char *pathJoinReplace(char **pDirname, char *basename);
int isNameExcluded(const char *pName);
int isDirExcluded(const char *pName, const struct stat64 *pStatbuf);
int isFileExcluded(const char *pName, const struct stat64 *pStatbuf);

#endif
