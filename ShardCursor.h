#pragma once

#include <string>
#include "History.h"
#include "Cursor.h"

using namespace std;

class ShardCursor
{
public:
	ShardCursor(History *H,std::shared_ptr<::Cursor> C);
	~ShardCursor();

	void SetParent(History* H);
	void SetSeries(std::string S);
	int NextStepCost();

	int Next();
	time_t Timestamp();
	double Value();
protected:
	std::string Series;
	PCursor Cursor;
	History* Hist;
	bool EoF;
};

