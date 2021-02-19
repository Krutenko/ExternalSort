#include <iostream>
#include <fstream>
#include <sstream>
#include <queue>
#include <iomanip>
#include <thread>
#include <mutex>
#include <errno.h>
#ifdef _WIN32
#include <Windows.h>
#elif __linux__
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#endif
#define FOLDERNAME "parts"
#ifdef _WIN32
#define FILENAME "\\part"
#elif __linux__
#define FILENAME "/part"
#endif
#define OUTPUTNAME "sorted.txt"
#define SIZE_T 1000
#define CHUNK_NUM_T 50
#define SIZE_S 1000000000
#define CHUNK_NUM_S 500
class double_num
{
public:
    double number;
    uint32_t precision;
};
class Compare1
{
public:
    bool operator() (double_num num1, double_num num2)
    {
        return num1.number > num2.number;
    }
};
using namespace std;
priority_queue<double_num, vector<double_num>, Compare1> chunk_odd, chunk_even;
uint32_t parity = 0;
bool waiting_for_print;
mutex wfp_mutex;
uint32_t full_size;
uint32_t chunk_num;
uint32_t chunk_size;

class Compare2
{
public:
    bool operator() (pair<double_num, int> pair1, pair<double_num, int> pair2)
    {
        return pair1.first.number > pair2.first.number;
    }
};

uint32_t GetPrecision(string number)
{
    uint32_t i;
    bool minus = false;
    bool dot = false;
    uint32_t precision;
    for (i = 0; i < number.length(); i++)
        if (number[i] == '-')
            minus = true;
        else if (number[i] == '.')
            dot = true;
        else if (number[i] == 'e')
            break;
    precision = i;
    if (minus)
        precision--;
    if (dot)
        precision--;
    return precision;
}

string DoubleToString(double_num val)
{
    stringstream stream;
    stream << setprecision(val.precision) << val.number;
    return stream.str();
}

string IntToString(int val)
{
    stringstream stream;
    stream << val;
    return stream.str();
}

void CreatePart()
{
    int i, j;
    double_num val;
    for (i = 0; i < chunk_num; i++)
    {
        while (!waiting_for_print)
#ifdef _WIN32
            Sleep(10);
#elif __linux__
            usleep(10000);
#endif
        string output_file_name = string(FOLDERNAME) + string(FILENAME) + IntToString(i + 1) + ".txt";
        ofstream output_file(output_file_name.c_str());
        for (j = 0; j < chunk_size; j++)
        {
            if (i % 2 == 0)
            {
                val = chunk_even.top();
                chunk_even.pop();
            }
            else
            {
                val = chunk_odd.top();
                chunk_odd.pop();
            }
            output_file << setprecision(val.precision) << val.number << endl;
        }
        output_file.close();
        wfp_mutex.lock();
        waiting_for_print = false;
        wfp_mutex.unlock();
    }
}

int main(int argc, char *argv[])
{
    string output_file_name;
    if (argc < 3)
    {
        cout << "Missing argument. Specify input file and mode. Exiting..." << endl;
        exit(-1);
    }
    else if (argc > 4)
    {
        cout << "Too many arguments. Exiting..." << endl;
        exit(-1);
    }
    else if (argc == 4)
    {
        output_file_name = string(argv[3]);
    }
    else if (argc == 3)
    {
        output_file_name = string(OUTPUTNAME);
    }
    string mode(argv[1]);
    if (mode == "-s")
    {
        full_size = SIZE_S;
        chunk_num = CHUNK_NUM_S;
        chunk_size = SIZE_S / CHUNK_NUM_S;
    }
    else if (mode == "-t")
    {
        full_size = SIZE_T;
        chunk_num = CHUNK_NUM_T;
        chunk_size = SIZE_T / CHUNK_NUM_T;
    }
    else
    {
        cout << "Incorrect mode. Must be \"-s\" or \"-t\". Exiting..." << endl;
        exit(-1);
    }
    uint32_t i = 0;
    uint32_t j = 0;
    uint32_t k;
    string line;
    double_num val;
    waiting_for_print = false;
    errno = 0;
    ifstream input_file(argv[2]);
    if (errno != 0)
    {
        cout << "Error opening input file. Exiting..." << endl;
        exit(-1);
    }
#ifdef _WIN32
    CreateDirectoryA(FOLDERNAME, NULL);
    if (GetLastError() == ERROR_PATH_NOT_FOUND)
    {
        cout << "Error creating intermediate directory. Exiting..." << endl;
        input_file.close();
        exit(-1);
    }
#elif __linux__
    errno = 0;
    if (mkdir(FOLDERNAME, 0777) == -1)
    {
        if (errno != EEXIST)
        {
            cout << "Error creating intermediate directory. Exiting..." << endl;
            input_file.close();
            exit(-1);
        }
    }
#endif
    thread writer(CreatePart);
    while (input_file >> line)
    {
        try
        {
            val.number = stod(line);
        }
        catch (...)
        {
            val.number = 0;
        }
        val.precision = GetPrecision(line);
        if (DoubleToString(val) != line)
            continue;
        i++;
        if (i > full_size)
        {
            cout << "Input file contains more than " << full_size << " numbers. Processing only the first " << full_size << "..." << endl;
            break;
        }
        if (parity == 0)
            chunk_even.push(val);
        else
            chunk_odd.push(val);
        j++;
        if (j == chunk_size)
        {
            while (waiting_for_print)
#ifdef _WIN32
                Sleep(10);
#elif __linux__
                usleep(10000);
#endif
            wfp_mutex.lock();
            waiting_for_print = true;
            wfp_mutex.unlock();
            j = 0;
            parity = (parity + 1) % 2;
        }
    }
    if (i < full_size)
    {
        cout << "Input file contains less than " << full_size << " numbers. Exiting..." << endl;
        input_file.close();
        exit(-1);
    }
    writer.join();
    input_file.close();
    errno = 0;
    ofstream output_file(output_file_name.c_str());
    if (errno != 0)
    {
        cout << "Error creating output file. Exiting..." << endl;
        input_file.close();
        exit(-1);
    }
    priority_queue<pair<double_num, int>, vector<pair<double_num, int>>, Compare2> print_queue;
    vector<ifstream> handles(chunk_num);
    for (i = 0; i < chunk_num; i++)
    {
        string part_file_name = string(FOLDERNAME) + string(FILENAME) + IntToString(i + 1) + ".txt";
        errno = 0;
        handles[i].open(part_file_name.c_str());
        if (errno != 0)
        {
            cout << "Error creating intermediate file " << i + 1 << ". Exiting..." << endl;
            for (j = 0; j < i; j++)
                handles[j].close();
            output_file.close();
            handles.clear();
            exit(-1);
        }
        handles[i] >> line;
        val.number = stod(line);
        val.precision = GetPrecision(line);
        print_queue.push(pair<double_num, int>(val, i));
    }
    pair<double_num, int> min_pair;
    while (print_queue.size() > 0)
    {
        min_pair = print_queue.top();
        print_queue.pop();
        output_file << setprecision(min_pair.first.precision) << min_pair.first.number << endl;
        flush(output_file);
        if (handles[min_pair.second] >> line)
        {
            val.number = stod(line);
            val.precision = GetPrecision(line);
            print_queue.push(pair <double_num, int>(val, min_pair.second));
        }
    }
    for (i = 0; i < chunk_num; i++)
        handles[i].close();
    output_file.close();
    handles.clear();
    return 0;
}