#pragma once
#include <string>
#include <vector>
#include "BTree.h"
#include "DataStore.h"
#include "Historian.h"
using namespace std;

class SeriesLocation
{
	//ShardHandle *DStore;
	std::vector<std::streamoff> Offsets;
	BTree Node;
};

class Series
{
public:
	Series();

	std::string Name;
	bool Loaded;
	double RecentValue;
	time_t RecentTimestamp;

	SeriesLocation Location;

};

