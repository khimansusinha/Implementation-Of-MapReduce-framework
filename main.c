/* The implemetation of the test driver program. Don't change this file */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "mapreduce.h"
#include "usr_functions.h"

int str_is_decimal_num(char * str)
{
    int ret = 1;
    char * p = NULL;
    
    if (NULL == str)
    {
        printf("error: null input\n");
        return 0;
    }

    p = str;
    while (*p)
    {
        if (*p < '0' || *p > '9')
        {
            ret = 0;
            break;
        }

        p += 1;
    }

    return ret;
}

int is_regular_file(char * file_path)
{
    struct stat file_stat;

    if (-1 == stat(file_path, &file_stat))
    {
        return 0;
    }

    if (S_ISREG(file_stat.st_mode))
    {
        return 1;
    }

    return 0;
}

void print_usage(char * cmd_name)
{
    printf("Usage: %s \"counter\"|\"finder\" file_path split_num [word_to_find]\n", cmd_name);
}


int main(int argc, char * argv[])
{
    int i = 0, is_letter_counter = 0;
    
    MAPREDUCE_SPEC spec;
    MAPREDUCE_RESULT result;

    setbuf(stdout, NULL); // no bufferring for stdio

    if (argc < 4)
    {
        print_usage(argv[0]);
        exit(1);
    }

    /* argv[1] must be either "counter", meaning the "Letter counter" task,
       or "finder", meaning the "Word finder" task*/
    if (!strcmp(argv[1], "counter"))
    {
        is_letter_counter = 1;
    }
    else if (!strcmp(argv[1], "finder"))
    {
        is_letter_counter = 0;
        if (argc < 5) // there must be a argv[4], which is the word to find
        {
            print_usage(argv[0]);
            exit(1);
        }
    }
    else
    {
        print_usage(argv[0]);
        exit(1);
    }

    // argv[2] is the input data file
    if (!is_regular_file(argv[2]))
    {
        printf("Regular file %s does not exist.\n", argv[2]);
        exit(0);
    }

    // argv[3] is the number of the splits
    if (!str_is_decimal_num(argv[3]))
    {
        printf("%s is not a valide split size. It should be a decimal number. \n", argv[3]);
        exit(0);
    }


    spec.input_data_filepath = argv[2]; // argv[2] is the input data file
    spec.split_num = atoi(argv[3]); // argv[3] is the number of the splits

    if (is_letter_counter)
    {
        spec.map_func = letter_counter_map;
        spec.reduce_func = letter_counter_reduce;
        spec.usr_data = NULL;
    }
    else
    {
        spec.map_func = word_finder_map;
        spec.reduce_func = word_finder_reduce;
        spec.usr_data = argv[4]; // argv[4] is the word to find
    }

    result.filepath = "mr.rst"; // name of the output file (placed in the working directory)
    result.map_worker_pid = malloc(spec.split_num * sizeof(*result.map_worker_pid));
	if (NULL == result.map_worker_pid)
	{
        printf("Memory allocation failed!\n");
		exit(2);
	}
    
    mapreduce(&spec, &result); // run the mapreduce task

    // print the result
    printf("***** RESULT ***** \n");
    printf("Result file: %s\n", result.filepath);
    
    printf("Map worker pids: "); 
    for (i = 0; i < spec.split_num; i++) printf("%d ", result.map_worker_pid[i]); 
    printf("\n");

    printf("Reduce worker pid: %d\n", result.reduce_worker_pid);
    printf("Processing time (us): %d\n", result.processing_time);
    
    exit(0);
}

