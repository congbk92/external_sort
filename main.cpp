#include <iostream>
#include <algorithm>
#include <vector>
#include <fstream>
#include <string>
#include <chrono>
#include <thread>
#include <deque>
#include <condition_variable>

using namespace std;

inline bool wrapper_lexicographical_compare(const string& s1, const string& s2){
    return lexicographical_compare(s1.cbegin(), s1.cend(), s2.cbegin(), s2.cend());
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
        }
        outF.close();
        numRun++;
    }
    cout<<"FileWriter Finished"<<endl;
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
void MergedPhase(const string& inputFile, const string& outputFile, int numRun, long heapMemLimit){

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