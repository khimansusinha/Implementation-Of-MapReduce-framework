#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include "mapreduce.h"
#include "common.h"
#include<fcntl.h>
#include<unistd.h>
#include<string.h>
#include<ctype.h>
#include<sys/wait.h>

// add your code here ...
#define MAX_LINE_SIZE 1024

void calculate_chunk_boundaries(int fd, off_t *start_chunk_off, off_t *end_chunk_off, int num_chunks, DATA_SPLIT *chunk_data)
{
	int i = 0;
	off_t chunk_size = 0;
	off_t current_off = 0; 
	off_t f_size = lseek(fd, 0, SEEK_END);
	lseek(fd, 0, SEEK_SET);

	chunk_size = f_size/num_chunks;
	current_off = 0;

	for (i = 0; i < num_chunks; i++) {
		start_chunk_off[i] = current_off;
		current_off += chunk_size;

		if ( i < num_chunks - 1 ) {
			lseek(fd, current_off, SEEK_SET);

			//now might be possible chunk size is in between the line so extend the chunk till next \n
			char ch;
			while (read(fd, &ch, 1) > 0 && ch != '\n') {
				current_off++;
			}
			//move one more time to go to next char after \n
			current_off++;

			//might be possible in checking the next line you have crossed the file size so check it
			if(lseek(fd, 0, SEEK_CUR) >= f_size) {
				current_off = f_size;
			}
			end_chunk_off[i] = current_off;
			//chunk_data[i].size = current_off;
			chunk_data[i].size = end_chunk_off[i] - start_chunk_off[i];

		} else if ( i == num_chunks - 1) {
			end_chunk_off[i] = f_size;
			//chunk_data[i].size = f_size;
			chunk_data[i].size = end_chunk_off[i] - start_chunk_off[i];
		}
	}

	for (i = 0; i < num_chunks; i++) {
		//printf("chunk %d::--> start off:[%ld] -> end off:[%ld], chunk_data_size [%d]\n", i, start_chunk_off[i], end_chunk_off[i], chunk_data[i].size);
	}
	//printf("\n");

	return;
}

void mapreduce(MAPREDUCE_SPEC * spec, MAPREDUCE_RESULT * result)
{
	// add you code here ...
    
	struct timeval start, end;
	int pipefd[2];
 
	if (NULL == spec || NULL == result)
	{
		EXIT_ERROR(ERROR, "NULL pointer!\n");
	}
    
	gettimeofday(&start, NULL);

	// add your code here ...

	/*
	 * chunking:
	 * Get the number of split then first partition the input file into N roughly equal-sized split
	 * N = split_num
	 * calculate the size of the input file in bytes and then split it into split_num parts

	 * forking and mapping:
	 * fork the N number of processes
	 * leverage the functionality of open file descriptor inherited to its child process, inherited means its kind of
	 * call call by reference i.e. both will have the different file pointers with same fd but in different address space
	 * so movement in child will also move in parent similarly movement in parent will also move in child.
	 * to avoid this interelated movement we can again open input file inside each child process in read only mode. 

	 * pipe and reduce:
	 * only first child does the reduce work all other child including first does the map work.
	 * parent creates a pipe() at start and this pipe used to communucate to only the first child to start the reduce work
         * once all the worker child process have finished there map work.
	 * parent waits for n - 1 child to finish the work, all child closes its both read and write pipe descriptors
	 * except first child closes its write pipe descriptor, when first child finished its work its blocks on read() call of
	 * the pipe and all other child do exit(0), so parents gets exit signal for all the child other than first child
	 * now parent writes to write end of the pipe so that only the first process gets the message from the parent and
	 * calls reduce function, the parent waits again only on the first child pid to finish the reduce() work, once
	 * the first child finished the reduce work, the parent gets the exit(0) from the first child as well and the job done.
 
	 * Reducing:
	 * reduce function picks up each child process generated intermediate file, accumulate the result inside the final file.
	 * to accumulate, the reduce function reads whole file content inside an in memory counter array or a line buffer and
         * then follows the same steps for all the intermediate files sequencially in order based on 0.txt, 1.txt, 2.txt...
	 * and writes the accumulative results inside the mr.rst result file.
	 */

	//INPUT FILE OPEN:
	//printf("The number of split given by user in command line is %d\n", spec->split_num);
	int fd = open(spec->input_data_filepath, O_RDONLY);
	if ( fd < 0 ){
		printf("Error in opening file [%s]\n", spec->input_data_filepath);
		EXIT_ERROR(ERROR, "failed to open the input file\n");
	}

	//PIPE OPEN:
	if (pipe(pipefd) == -1 ) {
		printf("ERROR: creatign pipe in parent process\n");
		EXIT_ERROR(ERROR, "ERROR: creating pipe in parent process\n");
	}

	//INTERMEDIATE and RESULT FILE OPEN:
	//for each child process find chunk bundaries that needs to be processed
	off_t start_off_chunk[spec->split_num];
	off_t end_off_chunk[spec->split_num];
	DATA_SPLIT chunk_data[spec->split_num];
	int p_fd_in[spec->split_num];//intermediate file fd array
	int fd_in_num = spec->split_num;
	int result_fd_out;//final reduce result file fd

	for (int i = 0; i < fd_in_num; i++) {
		char filename[64];
		snprintf(filename, sizeof(filename), "%s%d%s", "mr-", i, ".itm");

		int fd_int = open(filename, O_RDWR | O_CREAT | O_TRUNC, 0644);
		if (fd_int == -1)  {
			printf("Error creating intermediate file to store map result for child index [%d]\n", i);
			EXIT_ERROR(ERROR, "Error creating intermediate file to store map result for child index [%d]\n", i);
		}
		//store it inside p_fd_in array
		p_fd_in[i] = fd_int;
		//printf("the fd of intermediate file storing is [%d]\n", p_fd_in[i]);
	}
	result_fd_out = open(result->filepath, O_RDWR | O_CREAT | O_TRUNC, 0644);
	if (result_fd_out == -1) {
		printf("Error creating result output file [%s]\n", result->filepath);
		EXIT_ERROR(ERROR, "Error creating result output file [%s]\n", result->filepath);
	}

	//CHUNK BOUNDARIES:
	calculate_chunk_boundaries(fd, start_off_chunk, end_off_chunk, spec->split_num, chunk_data);

	//FORKING: creating worker child processes to do map and reduce
	for (int i = 0; i < spec->split_num; i++) {
		pid_t pid = fork();
		if (pid < 0 ) {
			printf("Error in creating processes using fork\n");
			EXIT_ERROR(ERROR, "Error in creating processes using fork\n");
		}
		if (pid == 0) {

			//first handle the open pipe read amd write descriptors inherited inside the child
			if ( i != 0 ) {
				close(pipefd[0]);
				close(pipefd[1]);
			} else if (i == 0) {
				//close the write end of the pipe inside the fist child only
				close(pipefd[1]);
			}

			//each child process opens the main input file independently
			int child_fd = open(spec->input_data_filepath, O_RDONLY);
			if (child_fd < 0) {
				printf("Error opening main input file in child\n");
				EXIT_ERROR(ERROR, "Error opening main input file in child\n");
			}
			//now move to exact start of chunk for this child to process
			lseek(child_fd, start_off_chunk[i], SEEK_SET);
			chunk_data[i].fd = child_fd;

			//now call pointer to function present inside the spec for map worker function
			if (spec->usr_data != NULL) {
				chunk_data[i].usr_data = spec->usr_data;
			}
			spec->map_func(&chunk_data[i], p_fd_in[i]);

			//now child uses duplicated fd to process its own chunk
			//child_chunk_map_work(dup_fd, start_off_chunk[i], end_off_chunk[i], i);
			//now the first child only has to do the reduce work so first child can't exit now
			if (i == 0) {
				char signal;
				//block on read call on pipe to get signal from the parent
				read(pipefd[0], &signal, 1);

				//got the signal now close the read end of the pipe as well inside the first child
				close(pipefd[0]);
				//printf("Received signal from parent in first child to start the reduce process\n");

				spec->reduce_func(p_fd_in, fd_in_num, result_fd_out);

			}

			//exit from the child process so that parent will receive the signal
			exit(0);
		}
		else {
			//parent process
			result->map_worker_pid[i] = pid;
		}
	}

	result->reduce_worker_pid = result->map_worker_pid[0];

	//close the original file descriptor in the parent also
	close(fd);

	//now wait in parent for all child process to finish except for the first child
	for (int i = 1; i < spec->split_num; i++) {
		waitpid(result->map_worker_pid[i], NULL, 0);
	}	
	//printf("\n");
	//printf("In parent: all the child processes have finished there map work\n");


	//now notify the first child via the pipe to perform the reduce work
	//close read end of the pipe
	close(pipefd[0]);
	//write to write end of the pipe
	write(pipefd[1], "1", 1);
	//close write end of the pipe
	close(pipefd[1]);

	//now wait in parent for the first child to finish the reduce work as well
	waitpid(result->map_worker_pid[0], NULL, 0);

	//printf("The first child process pid [%d] has also finished the reduce work\n", result->map_worker_pid[0]);

	//parent closes all intermediate file fds
	for (int i = 0; i < spec->split_num; i++) {
		close(p_fd_in[i]);
	}
	//parent closes the result out fd
	close(result_fd_out);

	gettimeofday(&end, NULL);   

	result->processing_time = (end.tv_sec - start.tv_sec) * US_PER_SEC + (end.tv_usec - start.tv_usec);
}
