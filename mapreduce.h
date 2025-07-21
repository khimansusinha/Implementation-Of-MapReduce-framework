/* You don't need to change this file */

#ifndef _MAPREDUCE_H
#define _MAPREDUCE_H

/* The data split type */
typedef struct _data_split
{
    int fd;  /* The file descriptor of the input data file */
    int size; /* The size of the split */
    void * usr_data;  /* This field is used only by the "Word finder" program: it records the word to find in the input data file */
}DATA_SPLIT;

typedef struct _mapreduce_spec
{
    char * input_data_filepath; /* The path of the (large) input data file */
    int split_num; /* The number of splits */
    int (*map_func)(DATA_SPLIT * split, int fd_out); /* Function pointer to the user-defined map function */
    int (*reduce_func)(int * p_fd_in, int fd_in_num, int fd_out); /* Function pointer to the user-defined reduce function */
    void * usr_data; /* This field is used only by the "Word finder" program: it records the word to find in the input data file */
}MAPREDUCE_SPEC;

typedef struct _mapreduce_result
{
    char * filepath; /* The path of the result file */
    int processing_time; /* The time used (in microseconds) for the mapreduce task */
    int * map_worker_pid; /* To record the process IDs of the map worker processes */
    int reduce_worker_pid; /* To record the process ID of the reduce worker */
}MAPREDUCE_RESULT;


void mapreduce(MAPREDUCE_SPEC * spec, MAPREDUCE_RESULT * result);



#endif
