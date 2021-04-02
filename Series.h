#pragma once
#include <string>
#include <vector>
#include "BTree.h"
#include "DataStore.h"

#define TIMESTAMP time_t

using namespace std;

#pragma pack(push, 1)
class SeriesInfo
{
public:
	SeriesInfo();
	
	int Options;
	TIMESTAMP FirstSample;	
};
#pragma pack(pop)