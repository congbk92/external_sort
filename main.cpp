#include <iostream>
#include <algorithm>
#include <vector>
#include <fstream>
#include <string>
#include <future>
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

/*************/
class HeapMemoryManager{
    
};

class KwaysMergeManager{
public:
    KwaysMergeManager(int k, int heapMemForMergeRead, int batch_size) :
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
    vector<int> bufSizeKways; // store size of Kth's buffer
};

struct mmap_info{
    int fd;
    char* beginPos;
    char* endPos;
    char* currentPos;
    off_t size;
    string filename;

    mmap_info(){

    }

    ~mmap_info(){
        //cout<<"delete "<<filename<<endl;
        if (currentPos != NULL){
            cout<<filename<<" wasn't complete"<<endl;
        }
    }
};

bool open_mmap(mmap_info &info, const std::string& fileName, int flag, off_t size = -1){
    info.filename = fileName;
    // flag: https://man7.org/linux/man-pages/man2/open.2.html
    info.fd = open(fileName.c_str(), flag);
    if (info.fd == -1) {
        cout<<"Can't open file descriptor of"<<fileName<<endl;
        return false;
    }

    /* Advise the kernel of our access pattern.  */
    //posix_fadvise(info.fd, 0, 0, 1);  // FDADVICE_SEQUENTIAL

    if (size < 0){
        struct stat st;
        fstat(info.fd, &st);
        size = st.st_size;
    }
    info.size = size;

    info.beginPos = (char*) mmap(0, info.size, PROT_READ, MAP_SHARED, info.fd, 0);
    
    if (info.beginPos == MAP_FAILED) {
        close(info.fd);
        cout<<"Can't open mmap of"<<fileName<<endl;
        return false;
    }
    info.endPos = info.beginPos + info.size;
    info.currentPos = info.beginPos;

    //madvise(info.beginPos, info.size, MADV_SEQUENTIAL);

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

inline int readlines_mmap(mmap_info &info, vector<string>& lines, int batch_size){
    lines.clear();
    char *p = NULL;
    int out_size = 0;
    while (batch_size > out_size && info.currentPos)
    {  
        char* find_pos = (char*) memchr(info.currentPos, '\n', info.endPos - info.currentPos);
        char* endline_pos = find_pos ? find_pos+1 : info.endPos;
        int length = endline_pos - info.currentPos;
        out_size += length;
        char *p = info.currentPos;
        info.currentPos = find_pos ? find_pos+1:NULL;
        lines.emplace_back(string(p, length));
    }
    return out_size;
}

std::deque<vector<string>> inBuffer;
std::deque<string> outBuffer;
std::mutex in_mutex;
std::mutex out_mutex;
std::condition_variable in_cv;
std::condition_variable out_cv;
bool reader_finished = false;
bool sorter_finished = false;

void FileReader(const string& inputFile, long heapMemLimit){
    mmap_info mmapInput;
    open_mmap(mmapInput, inputFile, O_RDONLY);

    long maxBatchSize = heapMemLimit/(2*MAX_IN_BUFFER_SIZE+2);
    int lines = 0;
    while (mmapInput.currentPos){
        vector<string> buffer;
        long heapMemConsume = 0;

        //Load
        while (mmapInput.currentPos && heapMemConsume < maxBatchSize){
            int length = 0;
            char* p = readline_mmap(mmapInput, length);
            heapMemConsume += length;
            buffer.push_back(string(p, length));
            lines++;
        }
        //cout<<buffer.back();

        {
            std::unique_lock<std::mutex> in_lock(in_mutex);
            //When the queue is full, it returns false, it has been blocked in this line
            in_cv.wait(in_lock, [] {return inBuffer.size() < MAX_IN_BUFFER_SIZE; });
            inBuffer.push_front(move(buffer));
            reader_finished = mmapInput.currentPos ? false : true;
        }
        in_cv.notify_one();
    }
    close_mmap(mmapInput);
    cout<<"FileReader Finished lines = "<<lines<<endl;
}

void Sorter(){
    int numLine = 0;
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
        string tmp;
        for(auto &x : buffer){ tmp += x; numLine++;}
        
        {
            std::unique_lock<std::mutex> out_lock(out_mutex);
            //When the queue is full, it returns false, it has been blocked in this line
            out_cv.wait(out_lock, [] {return outBuffer.size() < MAX_OUT_BUFFER_SIZE; });
            outBuffer.push_front(move(tmp));
            sorter_finished = isDone;
        }
        out_cv.notify_one();
    }
    cout<<"Sorter Finished, numline = "<<numLine<<endl;
}

void FileWriter(const string& inputFile, int& numRun){
    bool isDone = false;
    numRun = 0;
    while(!isDone) {
        string buffer;
        {
            std::unique_lock<std::mutex> out_lock(out_mutex);
            //When the queue is full, it returns false, it has been blocked in this line
            out_cv.wait(out_lock, [] {return !outBuffer.empty(); });
            buffer = outBuffer.back();
            outBuffer.pop_back();
            isDone = (sorter_finished && (outBuffer.size() == 0));
        }
        out_cv.notify_one();
        //Store
        FILE * pFile;
        pFile = fopen ((inputFile + "_0_" + to_string(numRun)).c_str(), "wb");
        fwrite (buffer.c_str() , sizeof(char), buffer.size(), pFile);
        fclose (pFile);
        numRun++;
    }
}

int InitialPhase(const string& inputFile, long heapMemLimit){
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    int numRun = 0;
    std::thread t1(FileReader, inputFile, heapMemLimit);
	std::thread t2(Sorter);
    std::thread t3(FileWriter, inputFile, std::ref(numRun));
	t1.join();
	t2.join();
    t3.join();
    cout<<"inBuffer size = "<<inBuffer.size()<<endl;
    cout<<"outBuffer size = "<<outBuffer.size()<<endl;

    std::deque<vector<string>>().swap(inBuffer);
    std::deque<string>().swap(outBuffer);
    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    std::cout << "InitialPhase Elapsed time = " << std::chrono::duration_cast<std::chrono::seconds>(end - begin).count() << "[s]" << std::endl;
    return numRun;
}

queue<string> meregedOutBuffer;
bool merger_finished = false;

void MergedFileWriter(const string& outputFile, int heapMemLimit){
    //int block_size = 4096;
    
    int block_size = getpagesize();
    int batch_size = (heapMemLimit/2/block_size)*block_size;
    batch_size = batch_size > block_size ? batch_size : block_size;

    bool isDone = false;
    //string buffer;
    //buffer.reserve(batch_size);
    FILE * pFile;
    pFile = fopen (outputFile.c_str(), "wb");

    while(!isDone) {
        string tmp_str;
        {
            std::unique_lock<std::mutex> out_lock(out_mutex);
            //When the queue is full, it returns false, it has been blocked in this line
            out_cv.wait(out_lock, [] {return !meregedOutBuffer.empty(); });
            tmp_str = meregedOutBuffer.back();
            meregedOutBuffer.pop();
            isDone = (merger_finished && meregedOutBuffer.empty());
        }
        out_cv.notify_one();
        fwrite (tmp_str.c_str() , sizeof(char), tmp_str.size(), pFile);
        /*
        if ((buffer.size() + tmp_str.size() <=  batch_size) || isDone){
            buffer += tmp_str;
        } else if (buffer.size() + tmp_str.size() <  batch_size) {
            buffer += subtmp_str.size()
        }*/
        /*
        buffer += tmp_str;
        if (buffer.size() >= batch_size) {
            fwrite (buffer.c_str() , sizeof(char), batch_size, pFile);
            buffer = buffer.substr(batch_size);
            buffer.reserve(batch_size);
        }
        if (isDone){
            fwrite (buffer.c_str() , sizeof(char), buffer.size(), pFile);
        }*/
    }
    fclose(pFile);
    //cout<<"MergedFileWriter Finished"<<endl;
}



// Merge k files:  ${inputFile}_${step}_${base} -> ${inputFile}_${step}_${base + k}
void KWayMerged(int k, int base, const string& inputFile, long heapMemLimit){
    //vector<string> outBuffer;
    vector<mmap_info> mmapInfos(k);
    vector<pair<int, string>> heapLines(k);
    vector<vector<string>> linesBuffer(k);
    int block_size = getpagesize();
    int batch_size = (heapMemLimit/2/(k+1)/block_size)*block_size;
    batch_size = batch_size > block_size ? batch_size : block_size;
    int out_buffer_size =(heapMemLimit/2/(MAX_OUT_BUFFER_SIZE+2)/block_size)*block_size;
    out_buffer_size = out_buffer_size > block_size ? out_buffer_size : block_size;
    KwaysMergeManager mergeMgr(k, heapMemLimit/2, batch_size);
    
    vector<string> futBuf;
    futBuf.reserve(batch_size);
    pair<int,future<int>> fut;

    //cout<<__FUNCTION__<<":"<<__LINE__<<endl;
    for (int i = 0; i < k; i++){
        //cout<<__FUNCTION__<<":"<<__LINE__<<" i = "<<i<<endl;
        open_mmap(mmapInfos[i], inputFile + "_" + to_string(i + base), O_RDONLY);
        //cout<<__FUNCTION__<<":"<<__LINE__<<" i = "<<i<<endl;
        int out_size = readlines_mmap(mmapInfos[i], linesBuffer[i], batch_size);
        //cout<<__FUNCTION__<<":"<<__LINE__<<" i = "<<i<<" out_size"<<out_size<<" batch_size="<<batch_size<<endl;
        heapLines[i] = {i, move(linesBuffer[i].back())}; // copy to buffer
        //cout<<__FUNCTION__<<":"<<__LINE__<<" i = "<<i<<endl;
        linesBuffer[i].pop_back();
        //cout<<__FUNCTION__<<":"<<__LINE__<<" i = "<<i<<endl;
        mergeMgr.AddToBuffer(out_size, i);
    }

    //cout<<__FUNCTION__<<":"<<__LINE__<<endl;

    std::make_heap (heapLines.begin(),heapLines.end(), wrapper_lexicographical_compare_1);
    string buffer;
    buffer.reserve(out_buffer_size);
    while(heapLines.size()) {
        std::pop_heap(heapLines.begin(),heapLines.end(), wrapper_lexicographical_compare_1);
        int mmapIdx = heapLines.back().first;
        buffer += heapLines.back().second;
        heapLines.pop_back();

        //Update buffer
        if (linesBuffer[mmapIdx].size() == 0) {
            /*if(fut.first == mmapIdx && fut.second.valid()){
                // shouldn't go to this
                fut.second.wait();
                int out_size = fut.second.get();
                linesBuffer[fut.first].insert(linesBuffer[fut.first].begin(), futBuf.begin(), futBuf.end());
                mergeMgr.AddToBuffer(out_size, fut.first);
            }else */if (mmapInfos[mmapIdx].currentPos) {
                // shouldn't go to this
                int out_size = readlines_mmap(mmapInfos[mmapIdx], linesBuffer[mmapIdx], batch_size);
                mergeMgr.AddToBuffer(out_size, mmapIdx);
            } else{
                close_mmap(mmapInfos[mmapIdx]);
                remove((inputFile + "_" + to_string(mmapIdx + base)).c_str());
            }
        }

        //Load from buf to heap
        if (linesBuffer[mmapIdx].size() > 0){
            mergeMgr.RemoveFromBuffer(linesBuffer[mmapIdx].back().size(), mmapIdx);
            heapLines.push_back({mmapIdx, move(linesBuffer[mmapIdx].back())}); // copy to buffer
            std::push_heap (heapLines.begin(), heapLines.end(), wrapper_lexicographical_compare_1);
            linesBuffer[mmapIdx].pop_back();
        }
        /*
        //Pre-load
        if (mergeMgr.shouldPreload()){
            if (fut.second.valid() && fut.second.wait_for(std::chrono::seconds(0)) == std::future_status::ready){
                int out_size = fut.second.get();
                linesBuffer[fut.first].insert(linesBuffer[fut.first].begin(), futBuf.begin(), futBuf.end());
                mergeMgr.AddToBuffer(out_size, fut.first);
            }
            //Call async
            int idxShortestBuf = mergeMgr.getShortestBufIdx();
            if (!fut.second.valid() && mmapInfos[idxShortestBuf].currentPos){
                fut.first = idxShortestBuf;
                fut.second = async(launch::async ,readlines_mmap, ref(mmapInfos[idxShortestBuf]), ref(futBuf), batch_size);
            }
        }*/

        if (buffer.size() > out_buffer_size || heapLines.empty()) {
            {
                std::unique_lock<std::mutex> out_lock(out_mutex);
                //When the queue is full, it returns false, it has been blocked in this line
                out_cv.wait(out_lock, [] {return meregedOutBuffer.size() < MAX_OUT_BUFFER_SIZE; });
                // Block in here
                meregedOutBuffer.push(buffer);
                buffer.clear();
                merger_finished = heapLines.empty();
            }
            out_cv.notify_one();
        }
    }
    //cout<<"KWayMerged Finished"<<endl;
}

void MergedPhase(const string& inputFile, const string& outputFile, int numRun, long heapMemLimit){
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    int step = 0;
    int base = 0;
    int numRunStep = 0; // number run at this step
    string outputMergedFile;
    while (numRun > 1){
        int k = numRun - base > 200 ? 200 : numRun - base; //To do
        if (k < numRun){
            outputMergedFile = inputFile + "_" + to_string(step+1) + "_" + to_string(numRunStep);
        } else {
            outputMergedFile = outputFile;
        }

        string inputFileMergedFile = inputFile + "_" + to_string(step);
        cout<<inputFileMergedFile<<" "<<outputMergedFile<<" k = "<<k<<" step = "<<step<<" base = "<<base<<endl;

        std::thread t1(KWayMerged, k, base, inputFileMergedFile, heapMemLimit);
        std::thread t2(MergedFileWriter, outputMergedFile, heapMemLimit);
        t1.join();
        t2.join();
        cout<<"meregedOutBuffer size = "<<meregedOutBuffer.size()<<endl;
        queue<string>().swap(meregedOutBuffer);
        base += k;
        numRunStep++;
        if (base >= numRun){
            base = 0;
            numRun = numRunStep;
            numRunStep = 0;
            step++;
        }
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
    
    //Initial phase
    int numRun = InitialPhase(inputFile, memLimitSize);
    std::cout << "InitialPhase numRun =  "<<numRun<<std::endl;

    //Merged phase
    MergedPhase(inputFile, outputFile, numRun, memLimitSize);

    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    std::cout << "Elapsed time = " << std::chrono::duration_cast<std::chrono::seconds>(end - begin).count() << "[s]" << std::endl;
    return 1;
}