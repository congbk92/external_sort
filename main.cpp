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
#include <cstdio>

// for mmap
#include <sys/stat.h>
#include <sys/mman.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>

using namespace std;

/*************/
#define MAX_IN_BUFFER_SIZE 2
#define MAX_OUT_BUFFER_SIZE 2

/*************/
class MMapReader{  //for read purpose only
public:

    MMapReader() = default;

    ~MMapReader(){
        munmap(beginPos, size);
        close(fd);
        if (isTmpfile){
            remove(fileName.c_str());
        }
        //cout<<"Remove "<<fileName<<endl;
    }

    void MMapOpen(const string& openfileName, bool isInputTmpfile = false, off_t read_size = -1){
        isTmpfile = isInputTmpfile;
        fileName = openfileName;
        // flag: https://man7.org/linux/man-pages/man2/open.2.html
        fd = open64(fileName.c_str(), O_RDONLY);
        if (fd == -1) {
            cout<<"Can't open file descriptor of "<<fileName<<endl;
            terminate();
        }

        /* Advise the kernel of our access pattern.  */
        //posix_fadvise(fd, 0, 0, 1);  // FDADVICE_SEQUENTIAL

        if (read_size < 0){
            struct stat64 st;
            fstat64(fd, &st);
            read_size = st.st_size;
        }
        size = read_size;

        beginPos = (char*) mmap64(0, size, PROT_READ, MAP_SHARED, fd, 0);
        
        if (beginPos == MAP_FAILED) {
            close(fd);
            cout<<"Can't open mmap of "<<fileName<<" size = "<<size<<endl;
            terminate();
        }
        endPos = beginPos + size;
        currentPos = beginPos;

        //madvise(beginPos, size, MADV_SEQUENTIAL);
    }

    inline char* Readline(int& length){
        char* find_pos = (char*) memchr(currentPos, '\n', endPos - currentPos);
        char* endline_pos = find_pos ? find_pos+1 : endPos;
        length = endline_pos - currentPos;
        char *p = currentPos;
        currentPos = find_pos ? find_pos+1:NULL;
        return p;
    }

    bool Valid() const {
        return currentPos != NULL;
    }
private:
    string fileName;
    int fd;
    char* beginPos;
    char* endPos;
    char* currentPos;
    off_t size;
    bool isTmpfile;
};

class MergeManager {  // This class help to manage buffer of merger to preload data from disk
public:
    MergeManager(int k, int heapMemForMergeRead, int batch_size) :
        kWays(k),
        currentBufSize(0),
        heapMemForMergeRead(heapMemForMergeRead),
        batch_size(batch_size){
            bufSizeKways.resize(k,0);
        }
    
    void AddToBuffer(int add_size, int add_idx){
        currentBufSize += add_size;
        bufSizeKways[add_idx] += add_size;
    }

    void RemoveFromBuffer(int remove_size, int add_idx){
        currentBufSize -= remove_size;
        bufSizeKways[add_idx] -= remove_size;
    }

    int getShortestBufIdx() const{
        return min_element(bufSizeKways.begin(), bufSizeKways.end()) - bufSizeKways.begin();
    }

    int getSumBufSize() const {
        return currentBufSize;
    }

    bool shouldPreload() const {
        return heapMemForMergeRead - currentBufSize >= batch_size;
    }

private:
    int kWays;
    int currentBufSize; // size all buffer;
    int heapMemForMergeRead;
    int batch_size;
    vector<int> bufSizeKways; // store size of K buffer
};

/*----utility function-----------*/
string GetTmpFile(string prefixTmpFile, int runNumber){
    return prefixTmpFile + "_" + to_string(runNumber);
}

inline bool wrapper_lexicographical_compare(const string& s1, const string& s2){
    //return atoi(s1.c_str()) < atoi(s2.c_str());
    return lexicographical_compare(s1.cbegin(), s1.cend(), s2.cbegin(), s2.cend());
}

inline bool wrapper_lexicographical_compare_1(const pair<int, string>& s1, const pair<int, string>& s2){
    //return atoi(s1.second.c_str()) > atoi(s2.second.c_str());
    return !lexicographical_compare(s1.second.cbegin(), s1.second.cend(), s2.second.cbegin(), s2.second.cend());
}
/*-------------------------------------*/

std::deque<vector<string>> inBuffer;
std::deque<string> outBuffer;
std::mutex in_mutex;
std::mutex out_mutex;
std::condition_variable in_cv;
std::condition_variable out_cv;
bool reader_finished = false;
bool sorter_finished = false;
bool merger_finished = false;

void FileReader(const string& inputFile, long heapMemLimit){
    MMapReader mmapInput;

    mmapInput.MMapOpen(inputFile);

    long maxBatchSize = heapMemLimit/(2*MAX_IN_BUFFER_SIZE+2);
    int lines = 0;
    long FileReader_size = 0;
    while (mmapInput.Valid()){
        vector<string> buffer;
        long heapMemConsume = 0;

        //Load
        while (mmapInput.Valid() && heapMemConsume < maxBatchSize){
            int length = 0;
            char* p = mmapInput.Readline(length);
            heapMemConsume += length;
            buffer.push_back(string(p, length));
            lines++;
            FileReader_size += length;
        }
        //cout<<buffer.back();

        {
            std::unique_lock<std::mutex> in_lock(in_mutex);
            //When the queue is full, it returns false, it has been blocked in this line
            in_cv.wait(in_lock, [] {return inBuffer.size() < MAX_IN_BUFFER_SIZE; });
            inBuffer.push_front(move(buffer));
            reader_finished = mmapInput.Valid() ? false : true;
        }
        in_cv.notify_one();
    }
    cout<<"FileReader Finished, lines = "<<lines<<" FileReader_size = "<<FileReader_size<<endl;
}

void Sorter(){
    int numLine = 0;
    bool isDone = false;
    long Sorter_size = 0;
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
        string tmp;
        for(auto &x : buffer){ tmp += x; numLine++;}
        
        {
            std::unique_lock<std::mutex> out_lock(out_mutex);
            //When the queue is full, it returns false, it has been blocked in this line
            out_cv.wait(out_lock, [] {return outBuffer.size() < MAX_OUT_BUFFER_SIZE; });
            Sorter_size += tmp.size();
            outBuffer.push_front(move(tmp));
            sorter_finished = isDone;
        }
        out_cv.notify_one();
    }
    cout<<"Sorter Finished, numline = "<<numLine<<" Sorter_size = "<<Sorter_size<<endl;
}

void FileWriter(const string& outputFile, int& numRun){
    bool isDone = false;
    numRun = 0;
    long FileWriter_size = 0;
    while(!isDone) {
        string buffer;
        {
            std::unique_lock<std::mutex> out_lock(out_mutex);
            //When the queue is full, it returns false, it has been blocked in this line
            out_cv.wait(out_lock, [] {return !outBuffer.empty(); });
            swap(buffer, outBuffer.back());
            outBuffer.pop_back();
            isDone = (sorter_finished && outBuffer.empty());
        }
        out_cv.notify_one();
        //Store
        FILE * pFile;
        pFile = fopen (GetTmpFile(outputFile, numRun).c_str(), "wb+");
        if (pFile == NULL){
            cout<<"Can't open "<<outputFile<<" to write"<<endl;
        }
        FileWriter_size += buffer.size();
        fwrite (buffer.c_str() , sizeof(char), buffer.size(), pFile);
        fclose (pFile);
        numRun++;
    }
    cout<<"FileWriter Finished, FileWriter_size = "<<FileWriter_size<<endl;
}

int InitialPhase(const string& inputFile, const string& prefixTmpFile, long heapMemLimit){
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    int numRun = 0;
    std::thread t1(FileReader, inputFile, heapMemLimit);
	std::thread t2(Sorter);
    std::thread t3(FileWriter, prefixTmpFile, std::ref(numRun));
	t1.join();
	t2.join();
    t3.join();
    std::deque<vector<string>>().swap(inBuffer);
    std::deque<string>().swap(outBuffer);
    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    std::cout << "InitialPhase Elapsed time = " << std::chrono::duration_cast<std::chrono::seconds>(end - begin).count() << "[s]" << std::endl;
    return numRun;
}

void MergedFileWriter(const string& outputFile){
    //int block_size = 4096;
    bool isDone = false;
    string buffer;
    FILE * pFile;
    pFile = fopen (outputFile.c_str(), "wb+");
    if (pFile == NULL){
        cout<<"Can't open "<<outputFile<<" to write"<<endl;
    }
    long MergedFileWriter_size = 0;
    while(!isDone) {
        {
            std::unique_lock<std::mutex> out_lock(out_mutex);
            //When the queue is full, it returns false, it has been blocked in this line
            out_cv.wait(out_lock, [] {return !outBuffer.empty(); });
            buffer = outBuffer.back();
            outBuffer.pop_back();
            isDone = (merger_finished && outBuffer.empty());
        }
        out_cv.notify_one();
        MergedFileWriter_size += buffer.size();
        fwrite (buffer.c_str() , sizeof(char), buffer.size(), pFile);
    }
    fclose (pFile);
    cout<<"MergedFileWriter Finished, MergedFileWriter_size = "<<MergedFileWriter_size<<endl;
}

// Merge k files:  ${inputFile}_${step}_${base} -> ${inputFile}_${step}_${base + k}
void KWayMerged(int k, int base, const string& prefixTmpFile, long heapMemLimit){
    vector<MMapReader> kWayReaders(k);
    vector<pair<int, string>> heapLines(k);
    
    long KWayMerged_size = 0;
    int lines = 0;
    for (int i = 0; i < k; i++){
        kWayReaders[i].MMapOpen(GetTmpFile(prefixTmpFile, i + base), true);
        int length = 0;
        char* p = kWayReaders[i].Readline(length);
        heapLines[i] = {i, string(p, length)}; // copy to buffer
    }
    //cout<<__FUNCTION__<<":"<<__LINE__<<endl;
    std::make_heap (heapLines.begin(),heapLines.end(), wrapper_lexicographical_compare_1);
    string buffer;
    buffer.reserve(heapMemLimit/3);
    while(heapLines.size()) {
        std::pop_heap(heapLines.begin(),heapLines.end(), wrapper_lexicographical_compare_1);
        int outWayIdx = heapLines.back().first;
        buffer += heapLines.back().second;
        lines++;
        heapLines.pop_back();
        
        if (kWayReaders[outWayIdx].Valid()) {
            int length = 0;
            char* p = kWayReaders[outWayIdx].Readline(length);
            heapLines.push_back({outWayIdx, string(p, length)}); // copy to buffer
            std::push_heap (heapLines.begin(), heapLines.end(), wrapper_lexicographical_compare_1);
        }


        if (buffer.size() > heapMemLimit/3 || heapLines.empty()) {
            {
                std::unique_lock<std::mutex> out_lock(out_mutex);
                //When the queue is full, it returns false, it has been blocked in this line
                out_cv.wait(out_lock, [] {return outBuffer.size() < MAX_OUT_BUFFER_SIZE; });
                // Block in here
                KWayMerged_size += buffer.size();
                outBuffer.push_front(move(buffer));
                buffer.clear();
                merger_finished = heapLines.empty();
            }
            out_cv.notify_one();
        }
    }
    cout<<"KWayMerged Finished, lines = "<<lines<<" KWayMerged_size = "<<KWayMerged_size<<endl;
}

void MergedPhase(const string& prefixTmpFile, const string& outputFile, int numInitialRun, long heapMemLimit){
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    if (numInitialRun == 1) {
        if (rename(GetTmpFile(prefixTmpFile, 0).c_str(), outputFile.c_str()) != 0) {
            cout<<"Can't output file, temp output file was located at "<<GetTmpFile(outputFile, 0)<<endl;
        }
        return;
    }

    int base = 0;
    int runNum = numInitialRun;
    int numRemainRun = numInitialRun;
    while (numRemainRun > 1){
        int k = numRemainRun < 10 ? numRemainRun : 10; //To do
        numRemainRun = numRemainRun - k + 1;
        string actualOutputFile = numRemainRun == 1? outputFile : GetTmpFile(prefixTmpFile, runNum);
        std::thread t1(KWayMerged, k, base, outputFile , heapMemLimit);
        std::thread t2(MergedFileWriter, actualOutputFile);
        t1.join();
        t2.join();
        deque<string>().swap(outBuffer);
        base += k;
        runNum++;
    }

    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    std::cout << "MergedPhase Elapsed time = " << std::chrono::duration_cast<std::chrono::seconds>(end - begin).count() << "[s]" << std::endl;
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
    
    string prefixTmpFile = outputFile;
    //Initial phase
    int numRun = InitialPhase(inputFile, prefixTmpFile, memLimitSize);
    std::cout << "InitialPhase numRun =  "<<numRun<<std::endl;

    //Merged phase
    MergedPhase(prefixTmpFile, outputFile, numRun, memLimitSize);

    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    std::cout << "Elapsed time = " << std::chrono::duration_cast<std::chrono::seconds>(end - begin).count() << "[s]" << std::endl;
    return 1;
}