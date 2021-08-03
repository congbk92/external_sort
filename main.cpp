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
#include <list>

// for mmap
#include <sys/stat.h>
#include <sys/mman.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>

using namespace std;

/*************/
#define MAX_BATCH_SIZE 100000000  //100Mb
#define MIN_BUFFER_SIZE 2

/*************/

std::deque<vector<string>> inBuffer;
std::deque<string> outBuffer;
std::mutex in_mutex;
std::mutex out_mutex;
std::condition_variable in_cv;
std::condition_variable out_cv;
bool reader_finished = false;
bool sorter_finished = false;
bool merger_finished = false;

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

int GetInitialBatchSize(long memLimitedSize){
    int block_size = getpagesize();
    long init_batch_size = memLimitedSize/(2*MIN_BUFFER_SIZE+5);
    init_batch_size = init_batch_size < MAX_BATCH_SIZE ? init_batch_size : MAX_BATCH_SIZE;
    init_batch_size = init_batch_size > block_size ? init_batch_size : block_size;
    return (init_batch_size/block_size)*block_size;
}

size_t GetInitialBufferSize(long memLimitedSize){
    int num = GetInitialBatchSize(memLimitedSize)/memLimitedSize - 5;
    return max(num, MIN_BUFFER_SIZE); 
}

int GetMaxKWays(long memLimitedSize){
    int max_k = memLimitedSize/2/getpagesize()-1; //ensure each buff > 1 page
    return min(max_k, 500); //To Do
}

int GetMergeReaderBatchSize(long memLimitedSize, int k){
    int block_size = getpagesize();
    int batch_size = (memLimitedSize/2/(k+1)/block_size)*block_size;
    return max(batch_size,block_size);
}

int GetMergeWriterBatchSize(long memLimitedSize){
    int block_size = getpagesize();
    long out_buffer_size = memLimitedSize/2/(MIN_BUFFER_SIZE + 2);
    out_buffer_size = out_buffer_size < MAX_BATCH_SIZE ? out_buffer_size : MAX_BATCH_SIZE;
    out_buffer_size = out_buffer_size > block_size ? out_buffer_size : block_size;
    return (out_buffer_size/block_size)*block_size;
}

size_t GetMergeWriterBufferSize(long memLimitedSize){
    int num = memLimitedSize/GetMergeWriterBatchSize(memLimitedSize) - 2;
    return max(num, MIN_BUFFER_SIZE);
}

/*-------------------------------------*/

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

        beginPos = (char*) mmap64(NULL, size, PROT_READ, MAP_SHARED, fd, 0);
        
        if (beginPos == MAP_FAILED) {
            close(fd);
            cout<<"Can't open mmap of "<<fileName<<" size = "<<size<<endl;
            terminate();
        }
        endPos = beginPos + size;
        currentPos = beginPos;

        //madvise(beginPos, size, MADV_SEQUENTIAL);
    }

    pair<char*, int> Readline(){
        char* find_pos = (char*) memchr(currentPos, '\n', endPos - currentPos);
        char* endline_pos = find_pos ? find_pos+1 : endPos;
        int length = endline_pos - currentPos;
        char *p = currentPos;
        currentPos = find_pos ? find_pos+1:NULL;
        return {p,length};
    }

    template<typename T>
    int ReadLines(T& lines, int batch_size){
        int readedLength = 0;
        int cap = 0;
        while (cap < batch_size && IsValid()){
            pair<char*, int> line = Readline();
            lines.emplace_back(line.first, line.second);
            readedLength += line.second;
            cap += lines.back().capacity();
        }
        //cout<<__FUNCTION__<<":"<<__LINE__<<" cap = "<<cap<<" readLength = "<<readedLength<<endl;
        return readedLength;
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

void FileReader(const string& inputFile, int batch_size, size_t buffer_size){
    MMapReader mmapInput;
    mmapInput.MMapOpen(inputFile);
    while (mmapInput.IsValid()){
        vector<string> buffer;
        //Load
        mmapInput.ReadLines(buffer, batch_size);
        {
            std::unique_lock<std::mutex> in_lock(in_mutex);
            //When the queue is full, it returns false, it has been blocked in this line
            in_cv.wait(in_lock, [buffer_size] {return inBuffer.size() < buffer_size; });
            inBuffer.push_back(move(buffer));
            reader_finished = mmapInput.IsValid() ? false : true;
        }
        in_cv.notify_one();
    }
    cout<<"FileReader Finished"<<endl;
}

void Sorter(int batch_size, size_t buffer_size){
    bool isDone = false;
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
        tmp.reserve(batch_size);
        for(auto &x : buffer) tmp += x;
        vector<string>().swap(buffer);
        {
            std::unique_lock<std::mutex> out_lock(out_mutex);
            //When the queue is full, it returns false, it has been blocked in this line
            out_cv.wait(out_lock, [buffer_size] {return outBuffer.size() < buffer_size; });
            outBuffer.push_back(move(tmp));
            sorter_finished = isDone;
        }
        out_cv.notify_one();
    }
    cout<<"Sorter Finished"<<endl;
}

int FileWriter(const string& outputFile){
    int numRun = 0;
    bool isDone = false;
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
        fwrite (buffer.c_str() , sizeof(char), buffer.size(), pFile);
        fclose (pFile);
        numRun++;
    }
    cout<<"FileWriter Finished"<<endl;
    return numRun;
}

int InitialPhase(const string& inputFile, const string& prefixTmpFile, long heapMemLimit){
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    int init_batch_size = GetInitialBatchSize(heapMemLimit);
    size_t buffer_size = GetInitialBufferSize(heapMemLimit);
    std::thread t1(FileReader, inputFile, init_batch_size, buffer_size/2);
	std::thread t2(Sorter, init_batch_size, buffer_size/2);

    std::packaged_task<int(const string&)> writeTask(FileWriter);
    std::future<int> result = writeTask.get_future();
    std::thread t3(std::move(writeTask), prefixTmpFile);;
	t1.join();
	t2.join();
    t3.join();
    std::deque<vector<string>>().swap(inBuffer);
    std::deque<string>().swap(outBuffer);
    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    std::cout << "InitialPhase Elapsed time = " << std::chrono::duration_cast<std::chrono::seconds>(end - begin).count() << "[s]" << std::endl;
    return result.get();
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
        fwrite (buffer.c_str() , sizeof(char), buffer.size(), pFile);
    }
    fclose (pFile);
    cout<<"MergedFileWriter Finished"<<endl;
}

// Merge k files:  ${inputFile}_${step}_${base} -> ${inputFile}_${step}_${base + k}
void KWayMerged(int k, int base, const string& prefixTmpFile, int in_batch_size, size_t out_batch_size, size_t out_buff_size){
    vector<MMapReader> kWayReaders(k);
    vector<pair<int, string>> heapLines(k);
    vector<list<string>> linesBuffer(k); //To Do

    for (int i = 0; i < k; i++){
        kWayReaders[i].MMapOpen(GetTmpFile(prefixTmpFile, i + base), true);
        kWayReaders[i].ReadLines(linesBuffer[i], in_batch_size);  //To Do
        heapLines[i] = {i, move(linesBuffer[i].front())};
        linesBuffer[i].pop_front();
    }

    std::make_heap (heapLines.begin(),heapLines.end(), wrapper_lexicographical_compare_1);
    string buffer;
    string outLine;
    buffer.reserve(out_batch_size);
    while(heapLines.size()) {
        std::pop_heap(heapLines.begin(),heapLines.end(), wrapper_lexicographical_compare_1);
        int outWayIdx = heapLines.back().first;
        outLine = heapLines.back().second;
        heapLines.pop_back();
        
        if (linesBuffer[outWayIdx].size() == 0 && kWayReaders[outWayIdx].IsValid()) {
            kWayReaders[outWayIdx].ReadLines(linesBuffer[outWayIdx], in_batch_size);
        }

        if (linesBuffer[outWayIdx].size() != 0){
            heapLines.push_back({outWayIdx, move(linesBuffer[outWayIdx].front())}); // copy to buffer
            std::push_heap (heapLines.begin(), heapLines.end(), wrapper_lexicographical_compare_1);
            linesBuffer[outWayIdx].pop_front();
        }        

        // write to output buffer
        if (buffer.size() + outLine.size() > out_batch_size || heapLines.empty()) {
            {
                if (heapLines.empty()) buffer += outLine;
                std::unique_lock<std::mutex> out_lock(out_mutex);
                //When the queue is full, it returns false, it has been blocked in this line
                out_cv.wait(out_lock, [out_buff_size] {return outBuffer.size() < out_buff_size; });
                outBuffer.push_back(move(buffer));
                buffer.clear();
                buffer.reserve(out_batch_size);
                merger_finished = heapLines.empty();
            }
            out_cv.notify_one();
        }
        buffer += outLine;
    }
    cout<<"KWayMerged Finished, k = "<<k<<endl;
}

void MergedPhase(const string& prefixTmpFile, const string& outputFile, int numInitialRun, long heapMemLimit){
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    if (numInitialRun == 1) {
        if (rename(GetTmpFile(prefixTmpFile, 0).c_str(), outputFile.c_str()) != 0) {
            cout<<"Can't output file, temp output file was located at "<<GetTmpFile(outputFile, 0)<<endl;
        }
        return;
    }

    int max_k = GetMaxKWays(heapMemLimit);

    int base = 0;
    int runNum = numInitialRun;
    int numRemainRun = numInitialRun;
    while (numRemainRun > 1){
        int k = numRemainRun < max_k ? numRemainRun : max_k; //To do
        
        int in_batch_size = GetMergeReaderBatchSize(heapMemLimit, k);
        int out_batch_size = GetMergeWriterBatchSize(heapMemLimit);
        size_t out_buff_size = GetMergeWriterBufferSize(heapMemLimit);
        //int k = 2;
        numRemainRun = numRemainRun - k + 1;
        string actualOutputFile = numRemainRun == 1? outputFile : GetTmpFile(prefixTmpFile, runNum);
        std::thread t1(KWayMerged, k, base, outputFile , in_batch_size, out_batch_size, out_buff_size);
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
    MergedPhase(prefixTmpFile, outputFile, numRun, memLimitSize); //TO DO

    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    std::cout << "Elapsed time = " << std::chrono::duration_cast<std::chrono::seconds>(end - begin).count() << "[s]" << std::endl;
    return 1;
}