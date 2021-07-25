#include <iostream>
#include <algorithm>
#include <vector>
#include <fstream>
#include <string>
#include <chrono>
#include <thread>
#include <deque>
#include <queue>
#include <condition_variable>

// for mmap
#include <sys/stat.h>
#include <sys/mman.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>

using namespace std;

inline bool wrapper_lexicographical_compare(const string& s1, const string& s2){
    return lexicographical_compare(s1.cbegin(), s1.cend(), s2.cbegin(), s2.cend());
}

inline bool wrapper_lexicographical_compare_1(const pair<int, string>& s1, const pair<int, string>& s2){
    return !lexicographical_compare(s1.second.cbegin(), s1.second.cend(), s2.second.cbegin(), s2.second.cend());
}


/*************/
#define MAX_IN_BUFFER_SIZE 2
#define MAX_OUT_BUFFER_SIZE 2


std::deque<vector<string>> inBuffer;
std::deque<vector<string>> outBuffer;
std::mutex in_mutex;
std::mutex out_mutex;
std::condition_variable in_cv;
std::condition_variable out_cv;
bool reader_finished = false;
bool sorter_finished = false;

void FileReader(const string& inputFile, long heapMemLimit){
    ifstream inF(inputFile, ifstream::in);
    long maxBatchSize = heapMemLimit/(2*MAX_IN_BUFFER_SIZE+2);
    while (inF.good()){
        vector<string> buffer;
        long heapMemConsume = 0;

        //Load
        while (inF.good() && heapMemConsume < maxBatchSize){
            string line;
            getline(inF, line);
            heapMemConsume += line.size();
            buffer.push_back(move(line));
        }
        if (buffer.back().empty()){ //TODO
            buffer.pop_back();
        }

        {
            std::unique_lock<std::mutex> in_lock(in_mutex);
            //When the queue is full, it returns false, it has been blocked in this line
            in_cv.wait(in_lock, [] {return inBuffer.size() < MAX_IN_BUFFER_SIZE; });
            inBuffer.push_front(move(buffer));
            reader_finished = inF.good() ? false : true;
        }
        in_cv.notify_one();
    }
    cout<<"FileReader Finished"<<endl;
}

void Sorter(){
    bool isDone = false;
    while (!isDone) {
        vector<string> buffer;
        {
            std::unique_lock<std::mutex> in_lock(in_mutex);
            //When the queue is empty, it returns false, it has been blocked in this line
            in_cv.wait(in_lock, [] {return !inBuffer.empty(); });
            swap(buffer, inBuffer.back());
            inBuffer.pop_back();
            isDone = (reader_finished && (inBuffer.size() == 0));
        }
        in_cv.notify_one();

        //Sort
        sort(buffer.begin(), buffer.end(), wrapper_lexicographical_compare);
        //To Do: Try lock and add to internal buffer

        {
            std::unique_lock<std::mutex> out_lock(out_mutex);
            //When the queue is full, it returns false, it has been blocked in this line
            out_cv.wait(out_lock, [] {return outBuffer.size() < MAX_OUT_BUFFER_SIZE; });
            outBuffer.push_front(move(buffer));
            sorter_finished = isDone;
        }
        out_cv.notify_one();
    }
    cout<<"Sorter Finished"<<endl;
}

void FileWriter(const string& inputFile, int& numRun){
    bool isDone = false;
    numRun = 0;
    int numLine = 0;
    while(!isDone) {
        vector<string> buffer;
        {
            std::unique_lock<std::mutex> out_lock(out_mutex);
            //When the queue is full, it returns false, it has been blocked in this line
            out_cv.wait(out_lock, [] {return !outBuffer.empty(); });
            swap(buffer, outBuffer.back());
            outBuffer.pop_back();
            isDone = (sorter_finished && (outBuffer.size() == 0));
        }
        out_cv.notify_one();
        //Store
        ofstream outF(inputFile + "_" + to_string(numRun), ofstream::out);
        for(auto& x : buffer){
            outF<<x<<endl;
            numLine++;
        }
        outF.close();
        numRun++;
    }
    cout<<"FileWriter Finished, numline = "<<numLine<<endl;
}

int InitialPhase(const string& inputFile, long heapMemLimit){
    int numRun = 0;
    std::thread t1(FileReader, inputFile, heapMemLimit);
	std::thread t2(Sorter);
    std::thread t3(FileWriter, inputFile, std::ref(numRun));
	t1.join();
	t2.join();
    t3.join();
    return numRun;
}


/*************/
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
    char* endline_pos = find_pos ? find_pos+1 : info.endPos;
    length = endline_pos - info.currentPos;
    char *p = info.currentPos;
    info.currentPos = find_pos ? find_pos+1:NULL;
    return p;
}

void KWayMerged(int k, const string& inputFile, const string& outputFile, long heapMemLimit){
    vector<string> outBuffer;
    vector<mmap_info> mmapInfos(k);
    vector<pair<int, string>> heapLines(k);
    for (int i = 0; i < k; i++){
        bool canOpen = open_mmap(mmapInfos[i], inputFile + "_" + to_string(i), O_RDONLY);
        if (!canOpen) cout<<"Can't open "<<inputFile + "_"<<i<<endl;

        int length = 0;
        char* p = readline_mmap(mmapInfos[i], length);
        heapLines[i] = {i, string(p, length)}; // copy to buffer
    }

    std::make_heap (heapLines.begin(),heapLines.end(), wrapper_lexicographical_compare_1);

    while(heapLines.size()) {
        std::pop_heap(heapLines.begin(),heapLines.end(), wrapper_lexicographical_compare_1);
        int mmapIdx = heapLines.back().first;
        outBuffer.push_back(move(heapLines.back().second));
        heapLines.pop_back();
        
        if (mmapInfos[mmapIdx].currentPos) {
            int length = 0;
            char* p = readline_mmap(mmapInfos[mmapIdx], length);
            heapLines.push_back({mmapIdx, string(p, length)}); // copy to buffer
            std::push_heap (heapLines.begin(), heapLines.end(), wrapper_lexicographical_compare_1);
        } else{
            close_mmap(mmapInfos[mmapIdx]);
            //remove temp file
            //remove( (inputFile + "_" + to_string(mmapIdx)).c_str());
        }
    }

    int numLine = 0;
    ofstream outF(outputFile, ofstream::out);
    for(auto& x : outBuffer){
        outF<<x;
        numLine++;
    }
    cout<<"numLine = "<<numLine<<endl;
    outF.close();
}

void MergedPhase(const string& inputFile, const string& outputFile, int numRun, long heapMemLimit){
    KWayMerged(numRun, inputFile, outputFile, heapMemLimit);
}

int main(int argc, char **argv)
{
    // Check input
    if (argc < 4)
    {
        cout << "Invalid";
        return 0;
    }
    
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

    string inputFile(argv[1]);
    string outputFile(argv[2]);
    long memLimitSize = atol(argv[3]);
    ifstream inF(inputFile, ifstream::in);
    
    //Initial phase
    int numRun = InitialPhase(inputFile, memLimitSize);
    std::cout << "InitialPhase numRun =  "<<numRun<<std::endl;

    //Merged phase
    MergedPhase(inputFile, outputFile, numRun, memLimitSize);

    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    std::cout << "Elapsed time = " << std::chrono::duration_cast<std::chrono::seconds>(end - begin).count() << "[s]" << std::endl;
    return 1;
}