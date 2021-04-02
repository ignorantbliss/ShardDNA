/*
 * This file is part of PRODNAME.
 *
 * See the COPYRIGHT file at the top-level directory of this distribution
 * for details of code ownership.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#pragma once
#include "ConfigFile.h"
#include "LogListener.h"
#include "DataStore.h"
#include "BTree.h"
#include "TSPoint.h"
#include "ShardCursor.h"
#include "Series.h"
#include <string>
#include <vector>
#include <time.h>
using namespace std;

class History;

#define PSC std::shared_ptr<ShardCursor>
#define TIMESTAMP time_t

struct HistoryConfig
{
	std::string StorageFolder;
	int TimePerShard;
};

class Query
{
public:
	Query();

	std::vector<std::string> Channels;
	time_t from;
	time_t to;
	int MaxSamples;
	int Options;
};

class AggregatedQuery : public Query
{
public:
	AggregatedQuery();
	
	int SampleCount;
	enum Aggregation {avg, min, max};
	Aggregation Method;
};

class QueryCursor;

class History
{
public:
	History();
	~History();
	
	bool Init(HistoryConfig Cfg);	
	DStore GetShard(TIMESTAMP tm, bool create);
	DStore GetNextShard(TIMESTAMP tm);
	PCursor GetNextShardWith(TIMESTAMP tm,const char *series);

	bool SeriesExists(const char* name);
	SeriesInfo GetSeries(const char* name, SeriesInfo* SI);

	bool RecordValue(const char* name, double Value);
	bool RecordValue(const char* name, double Value, TIMESTAMP Stamp, int Options = 0);
	TSPoint GetLatest(const char* name);
	ShardCursor* GetHistory(const char* name, TIMESTAMP from, TIMESTAMP to);
	QueryCursor* GetHistory(Query Q);
	QueryCursor* GetHistory(AggregatedQuery Q);

	void ShardDiagnostics();

protected:
	std::string StoragePath;
	int TimePerShard;
	vector<DStore> OpenShards;
	DStore GetStore(const char* filename);

private:
	PBTREE Shards;
	PBTREE Series;
	DStore ShardStorage;
	DStore SeriesStorage;
	std::string BaseDir;
	std::string ConfigDir;

};

