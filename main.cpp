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
        cout<<"Can't open file descriptor of"<<fileName<<endl;
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
        cout<<"Can't open mmap of"<<fileName<<endl;
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
    //cout<<"FileReader Finished lines = "<<lines<<endl;
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
    //cout<<"Sorter Finished, numline = "<<numLine<<endl;
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
            swap(buffer, outBuffer.back());
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
    std::deque<vector<string>>().swap(inBuffer);
    std::deque<string>().swap(outBuffer);
    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    std::cout << "InitialPhase Elapsed time = " << std::chrono::duration_cast<std::chrono::seconds>(end - begin).count() << "[s]" << std::endl;
    return numRun;
}

queue<string> meregedOutBuffer;
bool merger_finished = false;

void MergedFileWriter(const string& outputFile){
    //int block_size = 4096;
    bool isDone = false;
    string buffer;
    FILE * pFile;
    pFile = fopen (outputFile.c_str(), "wb");

    while(!isDone) {
        {
            std::unique_lock<std::mutex> out_lock(out_mutex);
            //When the queue is full, it returns false, it has been blocked in this line
            out_cv.wait(out_lock, [] {return !meregedOutBuffer.empty(); });
            swap(buffer, meregedOutBuffer.back());
            meregedOutBuffer.pop();
            isDone = (merger_finished && meregedOutBuffer.empty());
        }
        out_cv.notify_one();
        fwrite (buffer.c_str() , sizeof(char), buffer.size(), pFile);
    }
    fclose (pFile);
    //cout<<"MergedFileWriter Finished"<<endl;
}

// Merge k files:  ${inputFile}_${step}_${base} -> ${inputFile}_${step}_${base + k}
void KWayMerged(int k, int base, const string& inputFile, long heapMemLimit){
    //vector<string> outBuffer;
    vector<mmap_info> mmapInfos(k);
    vector<pair<int, string>> heapLines(k);
    for (int i = 0; i < k; i++){
        open_mmap(mmapInfos[i], inputFile + "_" + to_string(i + base), O_RDONLY);

        int length = 0;
        char* p = readline_mmap(mmapInfos[i], length);
        heapLines[i] = {i, string(p, length)}; // copy to buffer
    }

    std::make_heap (heapLines.begin(),heapLines.end(), wrapper_lexicographical_compare_1);
    string buffer;
    buffer.reserve(heapMemLimit/3);
    while(heapLines.size()) {
        std::pop_heap(heapLines.begin(),heapLines.end(), wrapper_lexicographical_compare_1);
        int mmapIdx = heapLines.back().first;
        buffer += string(move(heapLines.back().second));
        heapLines.pop_back();
        
        if (mmapInfos[mmapIdx].currentPos) {
            int length = 0;
            char* p = readline_mmap(mmapInfos[mmapIdx], length);
            heapLines.push_back({mmapIdx, string(p, length)}); // copy to buffer
            std::push_heap (heapLines.begin(), heapLines.end(), wrapper_lexicographical_compare_1);
        } else{
            close_mmap(mmapInfos[mmapIdx]);
            //remove temp file
            remove((inputFile + "_" + to_string(mmapIdx + base)).c_str());
        }

        if (buffer.size() > heapMemLimit/3 || heapLines.empty()) {
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
        int k = numRun - base > 100 ? 100 : numRun - base; //To do
        if (k < numRun){
            outputMergedFile = inputFile + "_" + to_string(step+1) + "_" + to_string(numRunStep);
        } else {
            outputMergedFile = outputFile;
        }

        //cout<<outputMergedFile<<" k = "<<k<<" step = "<<step<<" base = "<<base<<endl;

        std::thread t1(KWayMerged, k, base, inputFile + "_" + to_string(step), heapMemLimit);
        std::thread t2(MergedFileWriter, outputMergedFile);
        t1.join();
        t2.join();
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