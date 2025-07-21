/* This file defines some helper macros that may be helpful. */

#ifndef _COMMON_H
#define _COMMON_H

#define SUCCESS  0
#define ERROR    -1

#define US_PER_SEC 1000000

#define DEBUG
#ifdef DEBUG
#define DEBUG_MSG(fmt, args...) printf("%s(): \t"fmt, __FUNCTION__, ##args)
#else
#define DEBUG_MSG(fmt, args...)
#endif

#define ERR_MSG(fmt, args...) printf("ERROR in %d:%s(): "fmt, __LINE__, __FUNCTION__, ##args)

#define EXIT_ERROR(v, fmt, args...) \
    do                              \
    {                               \
        ERR_MSG(fmt, ##args);       \
        exit(v);              \
    } while(0)

#define _EXIT_ERROR(v, fmt, args...) \
    do                               \
    {                                \
        ERR_MSG(fmt, ##args);        \
        _exit(v);              \
    } while(0)

#endif
