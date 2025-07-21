#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <ctype.h>

#include "common.h"
#include "usr_functions.h"

#define MAX_LINE_SIZE 1024

/* User-defined map function for the "Letter counter" task.  
   This map function is called in a map worker process.
   @param split: The data split that the map function is going to work on.
                 Note that the file offset of the file descripter split->fd should be set to the properly
                 position when this map function is called.
   @param fd_out: The file descriptor of the itermediate data file output by the map function.
   @ret: 0 on success, -1 on error.
 */

void child_chunk_map_work(int fd, off_t start_off, off_t end_off, int fd_out)
{
        //first move to start_off of your own chunk
        lseek(fd, start_off, SEEK_SET);

        //read the chunk starting from your local chunk start_off and process your own local chunk only
        //take 26 size bucket to store the char frequency
        int char_count[26] = {0};

	//printf ("child worker pid:: [%d] :: chunk :: start off [%ld] end off [%ld] \n", getpid(), start_off, end_off);
	off_t cur_off = lseek(fd, 0, SEEK_CUR);

	while ( cur_off < end_off ) {
		char line[MAX_LINE_SIZE];
		char ch;
		int index = 0;
		while (read(fd, &ch, 1) == 1) {
			if (ch == '\n') {
				break;
			}
			line[index++] = ch;
		}

                for (int i = 0; i < index; i++) {
                        if (isalpha(line[i])) {
                                char_count[tolower(line[i]) - 'a']++;
                        }
                }
		cur_off += index + 1;
        }

        FILE *out_fp = fdopen(fd_out, "w+");
        if (!out_fp) {
                printf("Error creating intermediate file to store map result for child process pid [%d]\n", getpid());
                EXIT_ERROR(ERROR, "Error creating intermediate file to store map result in child process pid [%d]\n", getpid());
        }
        //now iterate through the frequency array and write the result inside the interediate file
        for (int i = 0; i < 26; i++) {
                fprintf(out_fp, "%c %d\n", 'a'+i, char_count[i]);
        }
	fflush(out_fp);

        //don't close intermediate output file here
        //fclose(out_fp);
        //printf("Child PID:: [%d] : frequency written inside intermediate file.\n", getpid());
}

int letter_counter_map(DATA_SPLIT * split, int fd_out)
{
	// add your implementation here ...
	//first move to start_off of your own chunk
	int in_fd = split->fd;
	off_t start_off = lseek(in_fd, 0, SEEK_CUR);
	off_t end_off = start_off + split->size;

	child_chunk_map_work(in_fd, start_off, end_off, fd_out);

	return 0;
}

/* User-defined reduce function for the "Letter counter" task.  
   This reduce function is called in a reduce worker process.
   @param p_fd_in: The address of the buffer holding the intermediate data files' file descriptors.
                   The imtermeidate data files are output by the map worker processes, and they
                   are the input for the reduce worker process.
   @param fd_in_num: The number of the intermediate files.
   @param fd_out: The file descriptor of the final result file.
   @ret: 0 on success, -1 on error.
   @example: if fd_in_num == 3, then there are 3 intermediate files, whose file descriptor is 
             identified by p_fd_in[0], p_fd_in[1], and p_fd_in[2] respectively.

*/

void first_child_reduce_work(int * p_fd_in, int fd_in_num, int fd_out)
{
        int total_count[26] = {0};

        //read from each intermediate file and aggregate the result
        for (int i = 0; i < fd_in_num; i++) {
		int fd = p_fd_in[i];
		lseek(fd, 0, SEEK_SET);
                FILE *in_fp = fdopen(fd, "r+");
                if( !in_fp ) {
                        printf("Error in opening intermediate file.\n");
                        continue;
                }
                char ch;
                int count;
                while (fscanf(in_fp, "%c %d\n", &ch, &count) == 2) {
                        total_count[ch - 'a'] += count;
                }
		//do not close fd here
                //fclose(in_fp);
        }

        //now write aggreage ressult inside the result file
        FILE *out_fp = fdopen(fd_out, "w+");
        if (!out_fp) {
                printf("Error in opening result file.\n");
                EXIT_ERROR(ERROR, "Error in opening result file.\n");
        }

        //write down the aggreage in memory to inside the result file
        for (int i = 0; i < 26; i++) {
                fprintf(out_fp, "%c %d\n", 'a'+i, total_count[i]);
        }
	fflush(out_fp);

	//do not close fd here
        //fclose(out_fp);
        //printf("Reduce work completed by first child process pid [%d]\n", getpid());
}

int letter_counter_reduce(int * p_fd_in, int fd_in_num, int fd_out)
{
	// add your implementation here ...
	first_child_reduce_work(p_fd_in, fd_in_num, fd_out);

	return 0;
}

void child_chunk_word_finder_map_work(int fd, off_t start_off, off_t end_off, int fd_out, char *word_find)
{
        //first move to start_off of your own chunk
        lseek(fd, start_off, SEEK_SET);

	//printf ("child worker pid:: [%d] :: chunk :: start off [%ld] end off [%ld] \n", getpid(), start_off, end_off);
	off_t cur_off = lseek(fd, 0, SEEK_CUR);

        FILE *out_fp = fdopen(fd_out, "w+");
        if (!out_fp) {
                printf("Error creating intermediate file to store map result for child process pid [%d]\n", getpid());
                EXIT_ERROR(ERROR, "Error creating intermediate file to store map result in child process pid [%d]\n", getpid());
	}
 
	while ( cur_off < end_off ) {
		char line[MAX_LINE_SIZE];
		char ch;
		int index = 0;
                char word_space_pat[MAX_LINE_SIZE], word_comma_pat[MAX_LINE_SIZE], word_dot_pat[MAX_LINE_SIZE];
                int len = strlen(word_find);
                
                //check if the last character of word is already . or space or , then don't append it

                if (word_find[len - 1] != ',' || word_find[len - 1] != '.' || word_find[len -1] != ' ') {
                        snprintf(word_space_pat, sizeof(word_space_pat), "%s ", word_find);
                        snprintf(word_comma_pat, sizeof(word_comma_pat), "%s,", word_find);
                        snprintf(word_dot_pat, sizeof(word_dot_pat), "%s.", word_find);

                }
		while (read(fd, &ch, 1) == 1) {
			if (ch == '\n') {
				break;
			}
			line[index++] = ch;
		}
		line[index] = '\0';
                if ( strstr(line, word_space_pat) != NULL || strstr(line, word_comma_pat) != NULL || strstr(line, word_dot_pat) != NULL || (strcmp(line , word_find) == 0) ) {
                        fprintf(out_fp, "%s\n", line);

                }
		cur_off += index + 1;
        }
	fflush(out_fp);

        //don't close intermediate output file here
        //fclose(out_fp);
        //printf("Child PID:: [%d] : frequency written inside intermediate file.\n", getpid());
}

/* User-defined map function for the "Word finder" task.  
   This map function is called in a map worker process.
   @param split: The data split that the map function is going to work on.
                 Note that the file offset of the file descripter split->fd should be set to the properly
                 position when this map function is called.
   @param fd_out: The file descriptor of the itermediate data file output by the map function.
   @ret: 0 on success, -1 on error.
 */
int word_finder_map(DATA_SPLIT * split, int fd_out)
{
	// add your implementation here ...
 	int in_fd = split->fd;
	off_t start_off = lseek(in_fd, 0, SEEK_CUR);
	off_t end_off = start_off + split->size;
	char *word_find = split->usr_data;

	child_chunk_word_finder_map_work(in_fd, start_off, end_off, fd_out, word_find);

   
	return 0;
}

void first_child_word_finder_reduce_work(int * p_fd_in, int fd_in_num, int fd_out)
{
        //now write aggreage ressult inside the result file
        FILE *out_fp = fdopen(fd_out, "w+");
        if (!out_fp) {
                printf("Error in opening result file.\n");
                EXIT_ERROR(ERROR, "Error in opening result file.\n");
        }


        //read from each intermediate file and aggregate the result
        for (int i = 0; i < fd_in_num; i++) {
		int fd = p_fd_in[i];
		lseek(fd, 0, SEEK_SET);
                FILE *in_fp = fdopen(fd, "r+");
                if( !in_fp ) {
                        printf("Error in opening intermediate file.\n");
                        continue;
                }

		char line[MAX_LINE_SIZE];
                while (fscanf(in_fp, "%[^\n]", line) == 1) {
			fprintf(out_fp, "%s\n", line);
			//to consume new line move one character fp
			fgetc(in_fp);
		}

		//do not close fd here
                //fclose(in_fp);
        }

	fflush(out_fp);

	//do not close fd here
        //fclose(out_fp);
        //printf("Reduce work completed by first child process pid [%d]\n", getpid());
}

/* User-defined reduce function for the "Word finder" task.  
   This reduce function is called in a reduce worker process.
   @param p_fd_in: The address of the buffer holding the intermediate data files' file descriptors.
                   The imtermeidate data files are output by the map worker processes, and they
                   are the input for the reduce worker process.
   @param fd_in_num: The number of the intermediate files.
   @param fd_out: The file descriptor of the final result file.
   @ret: 0 on success, -1 on error.
   @example: if fd_in_num == 3, then there are 3 intermediate files, whose file descriptor is 
             identified by p_fd_in[0], p_fd_in[1], and p_fd_in[2] respectively.

*/
int word_finder_reduce(int * p_fd_in, int fd_in_num, int fd_out)
{
	// add your implementation here ...
	first_child_word_finder_reduce_work(p_fd_in, fd_in_num, fd_out);

	return 0;
}


