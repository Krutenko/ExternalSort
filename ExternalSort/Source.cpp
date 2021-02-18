#define _CRT_SECURE_NO_WARNINGS
#include <iostream>
#include <fstream>
#include <sstream>
#include <queue>
#include <iomanip>
#include <errno.h>
#ifdef _WIN32
#include <Windows.h>
#elif __linux__

#endif
#define FOLDERNAME "parts1"
#define FILENAME "\\part"
#define OUTPUTNAME "sorted.txt"
#define SIZE 1000000000
#define CHUNK_SIZE 2000000
using namespace std;

class Compare
{
public:
    bool operator() (pair<double, int> pair1, pair<double, int> pair2)
    {
        return pair1.first > pair2.first;
    }
};

string DoubleToString(double val)
{
    stringstream stream;
    stream << setprecision(10) << val;
    return stream.str();
}

string IntToString(int val)
{
    stringstream stream;
    stream << val;
    return stream.str();
}

void sortAndWrite(double* values, int ch_num)
{
    sort(values, values + CHUNK_SIZE);
    string outputFileName = string(FOLDERNAME) + string(FILENAME) + IntToString(ch_num) + ".txt";
    ofstream outputFile(outputFileName.c_str());
    for (int i = 0; i < CHUNK_SIZE; i++)
        outputFile << setprecision(10) << values[i] << endl;
    outputFile.close();
}

int main(int argc, char *argv[])
{
    string output;
    if (argc < 2)
    {
        cout << "Missing argument. Specify input file. Exiting..." << endl;
        return -1;
    }
    else if (argc > 3)
    {
        cout << "Too many arguments. Exiting..." << endl;
        return -1;
    }
    else if (argc == 3)
    {
        output = string(argv[2]);
    }
    else if (argc == 2)
    {
        output = string(OUTPUTNAME);
    }
    uint32_t i = 0;
    uint32_t ch_num = 0;
    double* chunk = new double[CHUNK_SIZE];
    string line;
    double val = 0;
    uint32_t curr = 0;
    bool unprocessedData = true;
    ifstream inputFile(argv[1]);
#ifdef _WIN32
    bool status = CreateDirectoryA(FOLDERNAME, NULL);
#elif __linux__

#endif
    while (inputFile >> line)
    {
        val = stod(line);
        if (DoubleToString(val) != line)
            continue;
        i++;
        if (i > SIZE)
        {
            cout << "Input file contains more than " << SIZE << " numbers. Processing only the first " << SIZE << "..." << endl;
            break;
        }
        chunk[curr++] = val;
        if (curr == CHUNK_SIZE)
        {
            ch_num++;
            sortAndWrite(chunk, ch_num);
            curr = 0;
        }
    }
    if (i < SIZE)
    {
        cout << "Input file contains less than " << SIZE << " numbers. Exiting..." << endl;
        return -1;
    }
    inputFile.close();
    delete[] chunk;
    priority_queue<pair<double, int>, vector<pair<double, int> >, Compare> minHeap;
    ifstream* handles = new ifstream[ch_num];
    double firstValue;
    for (i = 0; i < ch_num; i++)
    {
        string sortedInputFileName = string(FOLDERNAME) + string(FILENAME) + IntToString(i + 1) + ".txt";
        errno = 0;
        handles[i].open(sortedInputFileName.c_str());
        cout << i + 1 << " " << GetLastError() << " " << errno << endl;
        firstValue = 0;
        handles[i] >> firstValue;
        minHeap.push(pair<double, int>(firstValue, i));
    }
    errno = 0;
    ofstream outputFile;
    outputFile.open(output.c_str());
    double nextValue;
    pair<double, int> minPair;
    while (minHeap.size() > 0)
    {
        minPair = minHeap.top();
        minHeap.pop();
        outputFile << setprecision(10) << minPair.first << endl;
        nextValue = 0;
        flush(outputFile);
        if (handles[minPair.second] >> nextValue)
            minHeap.push(pair <double, int>(nextValue, minPair.second));
        else
            cout << minHeap.size() << endl;
    }
    for (i = 0; i < ch_num; i++)
        handles[i].close();
    outputFile.close();
    delete[] handles;
    return 0;
}