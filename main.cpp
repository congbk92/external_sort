#include <iostream>
#include <algorithm>
#include <vector>
#include <fstream>
#include <string>

using namespace std;

inline bool wrapper_lexicographical_compare(const string& s1, const string& s2){
    return lexicographical_compare(s1.cbegin(), s1.cend(), s2.cbegin(), s2.cend());
}

int main(int argc, char **argv)
{
    // Check input
    if (argc < 4)
    {
        cout << "Invalid";
        return 0;
    }
    else
    {
        string inputFile(argv[1]);
        string outputFile(argv[2]);
        long memLimitSize = atol(argv[3]);
        ifstream inF(inputFile, ifstream::in);
        
        vector<string> buffer;
        while (inF.good()){
            string line;
            getline(inF, line);
            buffer.push_back(line);
        }
        if (buffer.back().empty()){ //TODO
            buffer.pop_back();
        }
        inF.close();

        sort(buffer.begin(), buffer.end(), wrapper_lexicographical_compare);

        ofstream outF(outputFile, ofstream::out);
        for(auto& x : buffer){
            outF<<x<<endl;
        }
        outF.close();
    }

    return 1;
}