#include "pch.h"
#include "History.h"
#include "BTree.h"
#include "Cursor.h"
#include "QueryCursor.h"

#include <string>
#include <iostream>
#include <fstream>
#include <time.h>
#include <ctime>
using namespace std;

//#define _CRT_SECURE_NO_WARNINGS

History::History()
{
	TimePerShard = 60 * 60 * 24;
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
	TimePerShard = HC.TimePerShard;
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
	Shards->LeafTemplate.PayloadSize = 12;
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
	DStore ActiveShard = GetShard(CurrentTime,false);
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

	DStore ActiveShard = GetShard(CurrentTime,false);
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
	ShardCursor* SC = new ShardCursor(this, C);
	SC->SetSeries(series);
	return SC;
}

QueryCursor* History::GetHistory(Query Q)
{
	QueryCursor* Cursor = new QueryCursor();
	for (long x = 0; x < Q.Channels.size(); x++)
	{
		Cursor->AddChannel(std::shared_ptr<ShardCursor>(GetHistory(Q.Channels[x].c_str(), Q.from, Q.to)));
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

	DStore ActiveShard = GetShard(CurrentTime,true);

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

std::string ShardNameFromDate(time_t tmx)
{
	char buf[13];
	tm* t = localtime(&tmx);
	std::strftime(buf, 13, "%Y%m%d%H%M",t);
	return buf;
}

std::shared_ptr<DataStore> History::GetShard(time_t date,bool create)
{
	//char shardname[32];
	char shardname[13];
	memset(shardname, 0, 13);

	time_t CurrentTime = date;
	BTreeKey* Key = Shards->PointSearch((const char*)&CurrentTime, sizeof(time_t), BTREE_SEARCH_PREVIOUS);
	if (Key == NULL)
	{
		std::string SN = ShardNameFromDate(CurrentTime);
		Key = Shards->Add((const char*)&CurrentTime, sizeof(time_t), SN.c_str(), SN.size());		
		memcpy(shardname, SN.c_str(), SN.size());
	}
	else
	{
		time_t basis = *(time_t*)Key->Key();
		memcpy(shardname, Key->Payload(), 12);
		if (create == true)
		{
			if (basis > CurrentTime)
			{				
				std::string SN = ShardNameFromDate(CurrentTime);
				Key = Shards->Add((const char*)&CurrentTime, sizeof(time_t), SN.c_str(), SN.size());
				memcpy(shardname, SN.c_str(), SN.size());				
			}
			else
			{
				if ((CurrentTime - basis) > TimePerShard)
				{
					CurrentTime -= ((CurrentTime - basis) % TimePerShard);
					std::string SN = ShardNameFromDate(CurrentTime);
					Key = Shards->Add((const char*)&CurrentTime, sizeof(time_t), SN.c_str(), SN.size());
					memcpy(shardname, SN.c_str(), SN.size());
				}				
			}
		}
	}

	DataStore* ActiveShard = new DataStore();
	string Shd = StoragePath + "\\";
	Shd += shardname;
	Shd += ".shard";

	ActiveShard->OpenOrCreate(Shd.c_str());	
	std::shared_ptr<DataStore> Store(ActiveShard);

	/*if (Key != NULL)
		delete Key;*/

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

PCursor History::GetNextShardWith(time_t CurrentTime, const char *series)
{	
	char shardname[13];
	memset(shardname, 0, 13);
	
	while (true)
	{
		BTreeKey* Key = Shards->PointSearch((const char*)&CurrentTime, sizeof(time_t), BTREE_SEARCH_NEXT);
		if (Key == NULL)
		{
			return NULL;
		}
		else
		{
			memcpy(shardname, Key->Payload(), 12);
			CurrentTime = *(time_t *)Key->Key();

			DStore DS(new DataStore());	
			std::string ShardFile = StoragePath + "\\";
			ShardFile += shardname;
			ShardFile += ".shard";
			
			PBTREE PointList = std::shared_ptr<BTree>(new BTree());
			PointList->LeafTemplate.KeySize = 32;
			PointList->BranchTemplate.KeySize = 32;
			PointList->LeafTemplate.PayloadSize = sizeof(std::streamoff);
			PointList->BranchTemplate.PayloadSize = sizeof(std::streamoff);
			DS->OpenOrCreate(ShardFile.c_str());
			DS->Init();
			if (!PointList->Load(DS, DS->BlockIndexToOffset(1)))
			{
				return NULL;
			}

			//Find this series in the point list
			BTreeKey* Key = PointList->PointSearch(series, strlen(series), BTREE_SEARCH_EXACT);
			if (Key != NULL)
			{
				//Found the series in the shard! Let's go!
				DataStore* ActiveShard = new DataStore();
				string Shd = StoragePath + "\\";
				Shd += shardname;
				Shd += ".shard";

				ActiveShard->OpenOrCreate(Shd.c_str());
				ActiveShard->Init();
				std::shared_ptr<DataStore> Store(ActiveShard);

				PBTREE Timeseries(new BTree());
				Timeseries->LeafTemplate.KeySize = sizeof(time_t);
				Timeseries->BranchTemplate.KeySize = sizeof(time_t);
				Timeseries->LeafTemplate.PayloadSize = sizeof(double);
				Timeseries->BranchTemplate.PayloadSize = sizeof(std::streamoff);
				Timeseries->SplitMode = BTREE_SPLITMODE_END;
				std::streamoff offset = 0;
				memcpy(&offset, Key->Payload(), sizeof(offset));
				if (!Timeseries->Load(Store, offset))
				{
					return NULL;
				}
				PCursor Curse = Timeseries->FirstNode();										
				return Curse;
			}			
			else
			{
				//Series isn't in this shard - let's continue.
			}
		}
	}

	return NULL;
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