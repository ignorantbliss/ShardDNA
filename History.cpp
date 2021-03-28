#include "pch.h"
#include "History.h"
#include "BTree.h"
#include "Cursor.h"
#include "QueryCursor.h"

#include <string>
#include <iostream>
#include <fstream>
#include <time.h>
using namespace std;

History::History()
{	
}

History::~History()
{
}

bool History::Init(HistoryConfig HC)
{
	//Get defaults initialise log...
	

	//Get Shard Index File
	StoragePath = HC.StorageFolder;
	if (StoragePath == "")
	{
		StoragePath = BaseDir + "\\Data";
	}
	ShardStorage.reset(new DataStore());
	if (ShardStorage->OpenOrCreate((StoragePath + "/shard.index").c_str()) == false)
	{
		Logger::Global.Log(LOGLEVEL_CRITICAL, "Historian", "Shard Storage File Could Not Be Opened");
		return false;
	}

	Logger::Global.Log(LOGLEVEL_INFO, "Historian", "Loaded Shard Index File");
	Shards = std::shared_ptr<BTree>(new BTree());
	Shards->LeafTemplate.KeySize = sizeof(time_t);
	Shards->BranchTemplate.KeySize = sizeof(long long);
	Shards->LeafTemplate.PayloadSize = 32;
	Shards->BranchTemplate.PayloadSize = sizeof(std::streamoff);
	Shards->LeafTemplate.CompareInverse = true;
	Shards->BranchTemplate.CompareInverse = true;
	
	ShardStorage->Init();
	Shards->Load(ShardStorage, ShardStorage->BlockIndexToOffset(1));
	return true;
}

TSPoint History::GetLatest(const char* series)
{
	time_t CurrentTime = time(NULL);

	//DStore Shard = GetShard(CurrentTime);
	DStore ActiveShard = GetShard(CurrentTime);
	BTree PointList;
	PointList.LeafTemplate.KeySize = 32;
	PointList.BranchTemplate.KeySize = 32;
	PointList.LeafTemplate.PayloadSize = sizeof(std::streamoff);
	PointList.BranchTemplate.PayloadSize = sizeof(std::streamoff);
	//SetContext("Request");
	ActiveShard->Init();
	if (!PointList.Load(ActiveShard, ActiveShard->BlockIndexToOffset(1)))
	{
		return TSPoint::NullValue();
	}

	BTree Timeseries;
	Timeseries.LeafTemplate.KeySize = sizeof(time_t);
	Timeseries.BranchTemplate.KeySize = sizeof(time_t);
	Timeseries.LeafTemplate.PayloadSize = sizeof(double);
	Timeseries.BranchTemplate.PayloadSize = sizeof(std::streamoff);
	Timeseries.LeafTemplate.CompareInverse = true;
	Timeseries.BranchTemplate.CompareInverse = true;
	std::streamoff offset = 0;

	BTreeKey* Key = PointList.PointSearch(series, strlen(series), BTREE_SEARCH_EXACT);
	if (Key == NULL)
	{
		return TSPoint::NullValue();;
	}
	else
	{
		memcpy(&offset, Key->Payload(), sizeof(offset));
		delete Key;
		if (!Timeseries.Load(ActiveShard, offset))
		{
			return TSPoint::NullValue();
		}
	}

	Key = Timeseries.PointSearch(&CurrentTime, sizeof(CurrentTime), BTREE_SEARCH_PREVIOUS);
	if (Key == NULL)
	{
		return TSPoint::NullValue();;
	}

	TSPoint P(Key);
	delete Key;
	return P;
}

ShardCursor* History::GetHistory(const char* series, time_t from, time_t to)
{

	time_t CurrentTime = from;
	//std::vector<TSPoint> Points;

	DStore ActiveShard = GetShard(CurrentTime);
	PBTREE PointList = std::shared_ptr<BTree>(new BTree());
	PointList->LeafTemplate.KeySize = 32;
	PointList->BranchTemplate.KeySize = 32;
	PointList->LeafTemplate.PayloadSize = sizeof(std::streamoff);
	PointList->BranchTemplate.PayloadSize = sizeof(std::streamoff);
	//SetContext("Request");
	ActiveShard->Init();
	if (!PointList->Load(ActiveShard, ActiveShard->BlockIndexToOffset(1)))
	{
		return NULL;
	}

	std::shared_ptr<BTree> Timeseries(new BTree());
	Timeseries->LeafTemplate.KeySize = sizeof(time_t);
	Timeseries->BranchTemplate.KeySize = sizeof(time_t);
	Timeseries->LeafTemplate.PayloadSize = sizeof(double);
	Timeseries->BranchTemplate.PayloadSize = sizeof(std::streamoff);
	Timeseries->LeafTemplate.CompareInverse = true;
	Timeseries->BranchTemplate.CompareInverse = true;
	std::streamoff offset = 0;

	BTreeKey* Key = PointList->PointSearch(series, strlen(series), BTREE_SEARCH_EXACT);
	if (Key == NULL)
	{
		return NULL;
	}
	else
	{
		memcpy(&offset, Key->Payload(), sizeof(offset));
		delete Key;
		if (!Timeseries->Load(ActiveShard, offset))
		{
			return NULL;
		}
	}

	PCursor C;
	C = Timeseries->Search(&CurrentTime, sizeof(CurrentTime), BTREE_SEARCH_PREVIOUS);
	C->SetTree(Timeseries);	
	//C->SetParent(this);
	ShardCursor* SC = new ShardCursor(this, C);
	return SC;
}

QueryCursor* History::GetHistory(Query Q)
{
	QueryCursor* Cursor = new QueryCursor();
	for (long x = 0; x < Q.Channels.size(); x++)
	{
		//Cursor->AddChannel(GetHistory(Q.Channels[x].c_str(), Q.from, Q.to));

	}
	return Cursor;
}

QueryCursor* History::GetHistory(AggregatedQuery Q)
{
	return NULL;
}

bool History::RecordValue(const char* series, double Value, time_t CurrentTime)
{
	//Step 1 - Identify Shard...

	DStore ActiveShard = GetShard(CurrentTime);

	PBTREE PointList = std::shared_ptr<BTree>(new BTree);
	PointList->LeafTemplate.KeySize = 32;
	PointList->BranchTemplate.KeySize = 32;
	PointList->LeafTemplate.PayloadSize = sizeof(std::streamoff);
	PointList->BranchTemplate.PayloadSize = sizeof(std::streamoff);
	//SetContext("Request");
	ActiveShard->Init();
	if (!PointList->Load(ActiveShard, ActiveShard->BlockIndexToOffset(1)))
	{
		return false;
	}

	//BTree Timeseries;
	PBTREE Timeseries = std::shared_ptr<BTree>(new BTree);
	Timeseries->LeafTemplate.KeySize = sizeof(time_t);
	Timeseries->BranchTemplate.KeySize = sizeof(time_t);
	Timeseries->LeafTemplate.PayloadSize = sizeof(double);
	Timeseries->BranchTemplate.PayloadSize = sizeof(std::streamoff);
	Timeseries->SplitMode = BTREE_SPLITMODE_END;
	std::streamoff offset = 0;

	BTreeKey* Key = PointList->PointSearch(series, strlen(series), BTREE_SEARCH_EXACT);
	if (Key == NULL)
	{
		//cout << "USING NEW CHANNEL" << endl;
		offset = Timeseries->Create(ActiveShard);
		PointList->Add(series, strlen(series), &offset, sizeof(offset));
	}
	else
	{
		//cout << "USING EXISTING CHANNEL" << endl;
		memcpy(&offset, Key->Payload(), sizeof(offset));
		if (!Timeseries->Load(ActiveShard, offset))
		{
			return false;
		}
	}

	//cout << "ADDING TO TIME SERIES" << endl;
	Timeseries->Add((const char*)&CurrentTime, sizeof(time_t), (const char*)&Value, sizeof(Value));


	return false;
}

bool History::RecordValue(const char* series, double Value)
{
	//Step 1 - Identify Shard...
	time_t CurrentTime = time(NULL);

	return RecordValue(series, Value, CurrentTime);
}

std::shared_ptr<DataStore> History::GetShard(time_t date)
{
	char shardname[32];

	time_t CurrentTime = time(NULL);
	BTreeKey* Key = Shards->PointSearch((const char*)&CurrentTime, sizeof(time_t), BTREE_SEARCH_PREVIOUS);
	if (Key == NULL)
	{
		//cout << "USING NEW SHARD" << endl;
		Key = Shards->Add((const char*)&CurrentTime, sizeof(time_t), "00000", 5);
		memcpy(shardname, "00000", strlen("00000") + 1);
	}
	else
	{
		//Get Shard Name		
		//cout << "USING EXISTING SHARD" << endl;
		memcpy(shardname, Key->Payload(), 32);
	}

	DataStore* ActiveShard = new DataStore();
	string Shd = StoragePath + "\\";
	Shd += shardname;
	Shd += ".shard";

	ActiveShard->OpenOrCreate(Shd.c_str());	
	std::shared_ptr<DataStore> Store(ActiveShard);

	return Store;
}

std::shared_ptr<DataStore> History::GetNextShard(time_t date)
{
	char shardname[32];

	time_t CurrentTime = time(NULL);
	BTreeKey* Key = Shards->PointSearch((const char*)&CurrentTime, sizeof(time_t), BTREE_SEARCH_NEXT);
	if (Key == NULL)
	{
		return NULL;
	}
	else
	{
		//Get Shard Name		
		//cout << "USING EXISTING SHARD" << endl;
		memcpy(shardname, Key->Payload(), 32);
	}

	DataStore* ActiveShard = new DataStore();
	string Shd = StoragePath + "\\";
	Shd += shardname;
	Shd += ".shard";

	ActiveShard->OpenOrCreate(Shd.c_str());
	std::shared_ptr<DataStore> Store(ActiveShard);

	return Store;
}

#ifndef MAX_PATH
#define MAX_PATH 1024
#endif

std::shared_ptr<DataStore> History::GetNextStore(DStore DS)
{	
	char shardname[MAX_PATH];
	std::string FN = DS->FileName();
	memcpy(shardname, FN.c_str(), MAX_PATH);

	time_t CurrentTime = time(NULL);
	BTreeKey* Key = Shards->PointSearch((const char*)&CurrentTime, sizeof(time_t), BTREE_SEARCH_NEXT);
	if (Key == NULL)
	{
		return NULL;
	}
	else
	{
		//Get Shard Name		
		//cout << "USING EXISTING SHARD" << endl;
		memcpy(shardname, Key->Payload(), 32);
	}

	DataStore* ActiveShard = new DataStore();
	string Shd = StoragePath + "\\";
	Shd += shardname;
	Shd += ".shard";

	ActiveShard->OpenOrCreate(Shd.c_str());
	std::shared_ptr<DataStore> Store(ActiveShard);

	return Store;
}

Query::Query()
{
	this->MaxSamples = 0;
	this->Options = 0;
	this->from = 0;
	this->to = 0;
}

AggregatedQuery::AggregatedQuery()
{
	Query::Query();
	this->Method = Aggregation::avg;
	this->SampleCount = 100;
}