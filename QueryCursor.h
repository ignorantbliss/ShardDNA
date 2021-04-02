#pragma once

#include <vector>
#include "Cursor.h"
#include "ShardCursor.h"
#include "History.h"

struct QueryRow
{
	int ChannelID;
	TIMESTAMP Timestamp;
	double Value;
};

class QueryCursor
{	
public:
	QueryCursor();
	~QueryCursor();

	QueryRow GetRow();

	int Next();
	bool EoF();

protected:
	int Channels;
	std::vector<PSC> Cursors;	
	void AddChannel(PSC Curse);

	friend History;

private:
	int ActiveChannel;
	
	struct PendingNext
	{
		int Chan;
		std::streamoff offset;
	};

	vector<PendingNext> PendingOpps;
	bool Empty;

};

