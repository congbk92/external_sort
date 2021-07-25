#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <string.h>
#include <string>



struct mmap_info{
    int fd;
    char* beginPos;
    char* endPos;
    char* currentPos;
    off_t size;
};

bool open_mmap(mmap_info &info, const std::string& fileName, int flag, off_t size = -1){
    // flag: https://man7.org/linux/man-pages/man2/open.2.html
    info.fd = open(fileName.c_str(), flag);
    if (info.fd == -1) {
        return false;
    }
    if (size < 0){
        struct stat st;
        fstat(info.fd, &st);
        size = st.st_size;
    }
    info.size = size;

    info.beginPos = (char*) mmap(0, info.size, PROT_READ, MAP_PRIVATE, info.fd, 0);
    
    if (info.beginPos == MAP_FAILED) {
        close(info.fd);
        return false;
    }
    info.endPos = info.beginPos + info.size;
    info.currentPos = info.beginPos;

    return true;
}

void close_mmap(const mmap_info &info){
    munmap(info.beginPos, info.size);
    close(info.fd);
}

inline char* readline_mmap(mmap_info &info, int& length){
    char* find_pos = (char*) memchr(info.currentPos, '\n', info.endPos - info.currentPos);
    char* endline_pos = find_pos ? find_pos : info.endPos;
    length = endline_pos - info.currentPos;
    char *p = info.currentPos;
    info.currentPos = find_pos ? find_pos+1:NULL;
    return p;
}

int main(int argc, char *argv[])
{
    int i;
    int fd;
    char* map;  /* mmapped array of int's */

    mmap_info mmapInfo;
    if (!open_mmap(mmapInfo, "testcase/world192_1000.txt" , O_RDONLY)) {
        perror("Error opening file for reading");
        exit(EXIT_FAILURE);
    }

    int lines = 0;
    while (mmapInfo.currentPos){
        int length;
        readline_mmap(mmapInfo, length);
        //printf("%d %d\n", lines, length);
        lines++;
    }
    printf("%d\n", lines);

    close_mmap(mmapInfo);
    return 0;
}