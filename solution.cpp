//
//  main.cpp
//  Datadog Homework
//
//  Created by Bir Bahadur Khatri on 18/10/2021.
//

#include <iostream>
#include <fstream>
#include <string>
#include <map>
#include <vector>
#include <queue>
#include <ctime>

using namespace std;

// source code taken from : https://www.geeksforgeeks.org/convert-unix-timestamp-to-dd-mm-yyyy-hhmmss-format/
string unixTimeToHumanReadable(long long seconds){
    // Save the time in Human
    // readable format
    string ans = "";
 
    // Number of days in month
    // in normal year
    int daysOfMonth[] = { 31, 28, 31, 30, 31, 30,
                          31, 31, 30, 31, 30, 31 };
 
    long int currYear, daysTillNow, extraTime,
        extraDays, index, date, month, hours,
        minutes, secondss, flag = 0;
 
    // Calculate total days unix time T
    daysTillNow = seconds / (24 * 60 * 60);
    extraTime = seconds % (24 * 60 * 60);
    currYear = 1970;
 
    // Calculating current year
    while (daysTillNow >= 365) {
        if (currYear % 400 == 0
            || (currYear % 4 == 0
                && currYear % 100 != 0)) {
            daysTillNow -= 366;
        }
        else {
            daysTillNow -= 365;
        }
        currYear += 1;
    }
 
    // Updating extradays because it
    // will give days till previous day
    // and we have include current day
    extraDays = daysTillNow + 1;
 
    if (currYear % 400 == 0
        || (currYear % 4 == 0
            && currYear % 100 != 0))
        flag = 1;
 
    // Calculating MONTH and DATE
    month = 0;
    index = 0;
    if (flag == 1) {
        while (true) {
 
            if (index == 1) {
                if (extraDays - 29 < 0)
                    break;
                month += 1;
                extraDays -= 29;
            }
            else {
                if (extraDays
                        - daysOfMonth[index]
                    < 0) {
                    break;
                }
                month += 1;
                extraDays -= daysOfMonth[index];
            }
            index += 1;
        }
    }
    else {
        while (true) {
 
            if (extraDays
                    - daysOfMonth[index]
                < 0) {
                break;
            }
            month += 1;
            extraDays -= daysOfMonth[index];
            index += 1;
        }
    }
 
    // Current Month
    if (extraDays > 0) {
        month += 1;
        date = extraDays;
    }
    else {
        if (month == 2 && flag == 1)
            date = 29;
        else {
            date = daysOfMonth[month - 1];
        }
    }
 
    // Calculating HH:MM:YYYY
    hours = extraTime / 3600;
    minutes = (extraTime % 3600) / 60;
    secondss = (extraTime % 3600) % 60;
 
    ans += to_string(date);
    ans += "/";
    ans += to_string(month);
    ans += "/";
    ans += to_string(currYear);
    ans += " ";
    ans += to_string(hours);
    ans += ":";
    ans += to_string(minutes);
    ans += ":";
    ans += to_string(secondss);
 
    // Return the time
    return ans;
}
 

class HttpLogMonitor {
    
    map < string, int > frequencyOfSections;
    deque < long long > lastTwoMinuteLogs;
    long long lastTime;
    bool alert;
    double threshHold;
    double currentAverageQPS;
    
public:
    HttpLogMonitor (){
        frequencyOfSections.clear();
        lastTwoMinuteLogs.clear();
        lastTime = 1e18;
        alert = false;
        threshHold = 10.0;
        currentAverageQPS = 0.0;
    }
    /*
     Add a request hit by a section.
     */
    void addRequest(pair < long long, string > request) {
        lastTime = min(lastTime, request.first);
        
        frequencyOfSections[ request.second ]++;
        lastTwoMinuteLogs.push_back(request.first);
    }
    
    /*
     Print the Top hit section in every 10 seconds.
     */
    void checkTopHitIn10Seconds(long long time) {
        if( time - lastTime > 10 ) {
            map <string,int> :: iterator it;
            string section;
            int count = 0;
            for(it = frequencyOfSections.begin(); it!= frequencyOfSections.end(); it++) {
                if(it->second > count) {
                    count = it->second;
                    section = it->first;
                }
            }
            cout << unixTimeToHumanReadable(time) <<  ": Top hit( " << count << " times ) by the section: " << section << endl;
            frequencyOfSections.clear();
            
            lastTime = time;
        }
    }
    
    /*
     Delete logs which are older than 2 minutes.
     */
    void delete2MinutesOldLogs(long long time) {
        while(lastTwoMinuteLogs.size() && ( time - lastTwoMinuteLogs.front() > 120) ) {
            lastTwoMinuteLogs.pop_front();
        }
        currentAverageQPS = ( lastTwoMinuteLogs.size() * 1.0 )/ 120.0;
    }
    
    /*
     Check if the QPS frquency is over thresHold or under thresHold.
     */
    void checkForAlert(long long time) {
        if( currentAverageQPS > threshHold  ) {
            if(alert != true) {
                alert = true;
                // Date format is : DD:MM:YYYY:HH:MM:SS
                cout << "High traffic generated an alert - hits = " << currentAverageQPS << ", triggered at: " <<   unixTimeToHumanReadable(time) <<endl;
            }
        } else {
            if(alert == true) {
                alert = false;
                // Date format is : DD:MM:YYYY:HH:MM:SS
                cout << "High traffic alert recovered - hits = " << currentAverageQPS << ", recovered at: " <<   unixTimeToHumanReadable(time) <<endl;
            }
        }
    }
    
    /*
     Returns current average QPS in last two minutes.
     */
    double getCurrentAvgQPS() {
        return currentAverageQPS;
    }
};

// Parse HTTP log and parse time in milliseconds and section.
pair < long long, string > parseLog(string line) {
    
    int firstOccuranceOfSpaceSlace = (int) line.find(" /") + 2;
    int firstOccuranceOfApache = (int) line.find("apache") + 8;
    
    // stores time in milliseconds.
    long long time = 0;
    while(firstOccuranceOfApache < line.size() && line[firstOccuranceOfApache] != ',') {
        time = time*10 + (line[firstOccuranceOfApache] - '0');
        firstOccuranceOfApache++;
    }
    // strores the section from line.
    string section = "";
    while(firstOccuranceOfSpaceSlace < line.size() &&  line[firstOccuranceOfSpaceSlace] != ' ' && line[firstOccuranceOfSpaceSlace] != '/' ) {
        section += line[firstOccuranceOfSpaceSlace];
        firstOccuranceOfSpaceSlace++;
    }
    
    return make_pair(time, "/" + section);
}

// This fuctions ensures the correctness of the HttpLogMonitor class.
void UnitTest() {
    HttpLogMonitor httpLogMonitor = HttpLogMonitor();
    for(int i = 0; i < 1200; i++) {
        httpLogMonitor.addRequest(make_pair(1549573861, "/api"));
        httpLogMonitor.delete2MinutesOldLogs(1549573861);
    }
    // This means alert is not triggered yet.
    assert(httpLogMonitor.getCurrentAvgQPS() <= 10.0);
    
    httpLogMonitor.addRequest(make_pair(1549573861, "/api"));
    httpLogMonitor.delete2MinutesOldLogs(1549573861);
    
    // This means alert is triggered.
    assert(httpLogMonitor.getCurrentAvgQPS() > 10.0);
    
    
    httpLogMonitor.addRequest(make_pair(1549574861, "/api"));
    httpLogMonitor.delete2MinutesOldLogs(1549574861);
    
    // This means alert is dropped again.
    assert(httpLogMonitor.getCurrentAvgQPS() <= 10.0);
}

int main() {
    // read file
    ifstream myfile ("sample_csv.txt");
    
    UnitTest();

    if (myfile.is_open()) {

        string line;
        int flag = 0;
        HttpLogMonitor httpLogMonitor = HttpLogMonitor();
        while ( getline (myfile,line) ) {
          if(flag == 0) {
            flag = 1;
              // ignoring first line as that line is not a httplog.
            continue;
          }

         pair < long long, string > timeAndSection = parseLog(line);
         httpLogMonitor.addRequest(timeAndSection);
         httpLogMonitor.checkTopHitIn10Seconds(timeAndSection.first);
         httpLogMonitor.delete2MinutesOldLogs(timeAndSection.first);
         httpLogMonitor.checkForAlert(timeAndSection.first);
      }

      myfile.close();
    }
    else cout << "Unable to open file";
    return 0;
}
