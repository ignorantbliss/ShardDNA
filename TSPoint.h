#pragma once

#include <time.h>

class BTreeKey;

class TSPoint
{
public:
	TSPoint(BTreeKey* Key);
	TSPoint(double v, TIMESTAMP tm);
	TSPoint();

	TIMESTAMP Time;
	double Value;

	static TSPoint NullValue();
};

