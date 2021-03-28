#pragma once

#include <time.h>

class BTreeKey;

class TSPoint
{
public:
	TSPoint(BTreeKey* Key);
	TSPoint(double v, time_t tm);
	TSPoint();

	time_t Time;
	double Value;

	static TSPoint NullValue();
};

