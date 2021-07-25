#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <string.h>

//#define FILEPATH "/tmp/mmapped.bin"
#define FILEPATH "testcase/world192_1000.txt"

/*
struct mmap_info{
    int fd;
    char* beginPos;
    char* currentPos;
}


bool open_()
bool close_MMAP()
bool readlineMMAP()
*/
int main(int argc, char *argv[])
{
    int i;
    int fd;
    char* map;  /* mmapped array of int's */

    fd = open(FILEPATH, O_RDONLY);
    if (fd == -1) {
	perror("Error opening file for reading");
	exit(EXIT_FAILURE);
    }

    struct stat st;
    fstat(fd, &st);
    off_t size = st.st_size;
    //printf("%ld\n" , size);

    map = (char*) mmap(0, size, PROT_READ, MAP_PRIVATE, fd, 0);
    
    if (map == MAP_FAILED) {
        close(fd);
        perror("Error mmapping the file");
        exit(EXIT_FAILURE);
    }

    int lines = 0;
    for(char *p = map; (p = (char*) memchr(p, '\n', (map + size) - p)); ++p) {
        //printf("%.*s", (map + size) - p, p);
        ++lines;
    }
    printf("%d\n", lines);

    if (munmap(map, size) == -1) {
	    perror("Error un-mmapping the file");
    }
    close(fd);
    return 0;
}