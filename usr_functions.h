/* You don't need to change this file */

#ifndef _USR_FUNCTIONS_H
#define _USR_FUNCTIONS_H

#include "mapreduce.h"


int letter_counter_map(DATA_SPLIT * split, int fd_out);
int letter_counter_reduce(int * p_fd_in, int fd_in_num, int fd_out);

int word_finder_map(DATA_SPLIT * split, int fd_out);
int word_finder_reduce(int * p_fd_in, int fd_in_num, int fd_out);


#endif
