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
#include <future>
#include <climits>

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

    string Readline(){
        char* find_pos = (char*) memchr(currentPos, '\n', endPos - currentPos);
        char* endline_pos = find_pos ? find_pos+1 : endPos;
        int length = endline_pos - currentPos;
        char *p = currentPos;
        currentPos = find_pos ? find_pos+1:NULL;
        return string(p,length);
    }

    int Readlines(vector<string>& lines, int batch_size){
        int readedLength = 0;
        while (readedLength < batch_size && IsValid()){
            lines.push_back(Readline());
            readedLength += lines.back().length();
        }
        return readedLength;
    }

    int ReadSortedlines(deque<string>& lines, int batch_size){
        int readedLength = 0;
        while (readedLength < batch_size && IsValid()){
            lines.push_back(Readline());
            readedLength += lines.back().length();
        }
        return readedLength;
    }

    deque<string> ReadSortedlinesAsync(int batch_size){
        deque<string> lines;
        int readedLength = 0;
        while (readedLength < batch_size && IsValid()){
            lines.push_back(Readline());
            readedLength += lines.back().length();
        }
        return lines;
    }

    bool IsValid() const {
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
        preserveSize(0),
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

    int GetShortestBufIdx(const vector<MMapReader> &kWayReaders) const{
        int min_element = INT_MAX;
        int min_idx = -1;
        for (int i = 0; i < static_cast<int>(bufSizeKways.size()); i++){
            if (bufSizeKways[i] < min_element && kWayReaders[i].IsValid()){
                min_element = bufSizeKways[i];
                min_idx = i;
            }
        }
        return min_idx;
    }

    int GetSumBufSize() const {
        return currentBufSize;
    }

    bool ShouldPreload() const {
        return heapMemForMergeRead - currentBufSize - preserveSize >= batch_size;
    }

    void Preserve(){
        preserveSize += batch_size;
    }

    void Serve(){
        preserveSize -= batch_size;
    }


private:
    int kWays;
    int preserveSize; 
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
    return lexicographical_compare(s1.cbegin(), s1.cend(), s2.cbegin(), s2.cend());
}

inline bool wrapper_lexicographical_compare_1(const pair<int, string>& s1, const pair<int, string>& s2){
    return lexicographical_compare(s2.second.cbegin(), s2.second.cend(), s1.second.cbegin(), s1.second.cend());
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
    while (mmapInput.IsValid()){
        vector<string> buffer;
        //Load
        FileReader_size += mmapInput.Readlines(buffer, maxBatchSize);
        lines += buffer.size();

        {
            std::unique_lock<std::mutex> in_lock(in_mutex);
            //When the queue is full, it returns false, it has been blocked in this line
            in_cv.wait(in_lock, [] {return inBuffer.size() < MAX_IN_BUFFER_SIZE; });
            inBuffer.push_back(move(buffer));
            reader_finished = mmapInput.IsValid() ? false : true;
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
            swap(buffer, inBuffer.front());
            inBuffer.pop_front();
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
            outBuffer.push_back(move(tmp));
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
            swap(buffer, outBuffer.front());
            outBuffer.pop_front();
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
            buffer = outBuffer.front();
            outBuffer.pop_front();
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
    vector<deque<string>> linesBuffer(k);
    pair<int,future<deque<string>>> fut;

    int block_size = getpagesize();
    int batch_size = (heapMemLimit/2/(k+1)/block_size)*block_size;
    batch_size = batch_size > block_size ? batch_size : block_size;
    int out_buffer_size =(heapMemLimit/2/(MAX_OUT_BUFFER_SIZE+2)/block_size)*block_size;
    out_buffer_size = out_buffer_size > block_size ? out_buffer_size : block_size;
    
    cout<<"batch_size = "<<batch_size<<" out_buffer_size = "<<out_buffer_size<<endl;
    MergeManager mergeMgr(k, heapMemLimit/2, batch_size);

    long KWayMerged_size = 0;
    int lines = 0;
    for (int i = 0; i < k; i++){
        kWayReaders[i].MMapOpen(GetTmpFile(prefixTmpFile, i + base), true);
        int readedLength  = kWayReaders[i].ReadSortedlines(linesBuffer[i], batch_size);
        mergeMgr.AddToBuffer(readedLength-linesBuffer[i].front().length(), i);
        heapLines[i] = {i, move(linesBuffer[i].front())};
        linesBuffer[i].pop_front();
    }


    std::make_heap (heapLines.begin(),heapLines.end(), wrapper_lexicographical_compare_1);
    string buffer;
    buffer.reserve(out_buffer_size);
    int pre_loaded_run = 0;
    int pre_loaded_done = 0;
    int not_pre_loaded = 0;
    while(heapLines.size()) {
        std::pop_heap(heapLines.begin(),heapLines.end(), wrapper_lexicographical_compare_1);
        int outWayIdx = heapLines.back().first;
        buffer += heapLines.back().second;
        lines++;
        heapLines.pop_back();
        
        if (linesBuffer[outWayIdx].size() == 0) {
            // shouldn't go to this
            if(fut.first == outWayIdx && fut.second.valid()){
                fut.second.wait();
                deque<string> futBuf = fut.second.get();
                int readedLength = 0;
                for (auto &x : futBuf) readedLength += x.size();
                linesBuffer[outWayIdx].swap(futBuf);
                mergeMgr.AddToBuffer(readedLength, outWayIdx);
                mergeMgr.Serve();
                pre_loaded_run++;
            } else if (kWayReaders[outWayIdx].IsValid()){
                int readedLength = kWayReaders[outWayIdx].ReadSortedlines(linesBuffer[outWayIdx], batch_size);
                mergeMgr.AddToBuffer(readedLength, outWayIdx);
                not_pre_loaded++;
            }
        }

        if (linesBuffer[outWayIdx].size() != 0){
            mergeMgr.RemoveFromBuffer(linesBuffer[outWayIdx].back().length(), outWayIdx);
            heapLines.push_back({outWayIdx, move(linesBuffer[outWayIdx].front())}); // copy to buffer
            std::push_heap (heapLines.begin(), heapLines.end(), wrapper_lexicographical_compare_1);
            linesBuffer[outWayIdx].pop_front();
        }

        //Pre-load
        if (mergeMgr.ShouldPreload()){
            if (fut.second.valid() && fut.second.wait_for(std::chrono::seconds(0)) == std::future_status::ready){
                deque<string> futBuf = fut.second.get();
                int readedLength = 0;
                for (auto &x : futBuf) readedLength += x.size();
                linesBuffer[fut.first].insert(linesBuffer[fut.first].end(), futBuf.begin(), futBuf.end());
                mergeMgr.AddToBuffer(readedLength, fut.first);
                mergeMgr.Serve();
                pre_loaded_done++;
            }
            //Call async
            int idxShortestBuf = mergeMgr.GetShortestBufIdx(kWayReaders);
            if (!fut.second.valid() && idxShortestBuf >= 0 && idxShortestBuf < k){
                fut.first = idxShortestBuf;
                fut.second = async(launch::async, &MMapReader::ReadSortedlinesAsync, &kWayReaders[idxShortestBuf], batch_size);
                mergeMgr.Preserve();
            }
        }


        // write to output buffer
        if (static_cast<int>(buffer.size()) > out_buffer_size || heapLines.empty()) {
            {
                std::unique_lock<std::mutex> out_lock(out_mutex);
                //When the queue is full, it returns false, it has been blocked in this line
                out_cv.wait(out_lock, [] {return outBuffer.size() < MAX_OUT_BUFFER_SIZE; });
                // Block in here
                KWayMerged_size += buffer.size();
                outBuffer.push_back(move(buffer));
                buffer.clear();
                merger_finished = heapLines.empty();
            }
            out_cv.notify_one();
        }
    }
    cout<<"KWayMerged Finished, lines = "<<lines<<" KWayMerged_size = "<<KWayMerged_size<<endl;
    cout<<"pre_loaded_done = "<<pre_loaded_done<<" ,pre_loaded_run = "<<pre_loaded_run<<" ,not_pre_loaded = "<<not_pre_loaded<<endl;
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
        int k = numRemainRun < 200 ? numRemainRun : 200; //To do
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