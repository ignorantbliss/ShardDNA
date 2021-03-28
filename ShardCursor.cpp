#include "pch.h"
#include "Cursor.h"
#include "ShardCursor.h"

ShardCursor::ShardCursor(History* H, std::shared_ptr<::Cursor> C)
{
	Hist = H;
	Cursor = C;
	EoF = false;
}

ShardCursor::~ShardCursor()
{

}

int ShardCursor::Next()
{
	int Ret = Cursor->Next();
	if (Ret == CURSOR_END)
	{
		//Check to see if there is anything else in the next shard...
			
		DStore DS = Hist->GetNextStore(Cursor->Tree->GetDataStore());
		while (DS != NULL)
		{
			//Find the list of points inside this shard
			BTree PointList;
			PointList.LeafTemplate.KeySize = 32;
			PointList.BranchTemplate.KeySize = 32;
			PointList.LeafTemplate.PayloadSize = sizeof(std::streamoff);
			PointList.BranchTemplate.PayloadSize = sizeof(std::streamoff);
			DS->Init();
			if (!PointList.Load(DS, DS->BlockIndexToOffset(1)))
			{
				this->EoF = true;
				return CURSOR_END;
			}
			//Find this series in the point list
			BTreeKey* Key = PointList.PointSearch(Series.c_str(), Series.size(), BTREE_SEARCH_EXACT);
			if (Key == NULL)
			{
				//This series isn't present in this shard - keep trying
				DStore DS = Hist->GetNextStore(Cursor->Tree->GetDataStore());
				if (DS == NULL)
				{
					this->EoF = true;
					break;
				}
			}
			else
			{
				//Found the series - Create it
				std::streamoff offset;
				memcpy(&offset, Key->Payload(), sizeof(offset));

				BTree Timeseries;
				Timeseries.LeafTemplate.KeySize = sizeof(time_t);
				Timeseries.BranchTemplate.KeySize = sizeof(time_t);
				Timeseries.LeafTemplate.PayloadSize = sizeof(double);
				Timeseries.BranchTemplate.PayloadSize = sizeof(std::streamoff);
				Timeseries.SplitMode = BTREE_SPLITMODE_END;
				offset = 0;

				if (!Timeseries.Load(DS, offset))
				{
					this->EoF = true;
					break;
				}
				else
				{
					//Change the position to the first node of the series.					
					Cursor = Timeseries.FirstNode();
					break;
				}
			}

		}

		this->EoF = true;	
	}
	return Ret;
}

time_t ShardCursor::Timestamp()
{
	return *(time_t*)Cursor->Key();
}


double ShardCursor::Value()
{
	return *(double*)Cursor->Value();
}

void ShardCursor::SetParent(History* H)
{
	Hist = H;
}

void ShardCursor::SetSeries(std::string S)
{
	Series = S;
}

int ShardCursor::NextStepCost()
{
	if (this->EoF == true)
	{
		return 1;
	}
	if (Cursor->Index < Cursor->Active->KeyCount() - 1)
	{
		return 0;
	}
	else
	{
		if (Cursor->Active->Next == 0)
		{
			return 9999999;
		}
		return (int)(Cursor->Active->Next - Cursor->Offset);
	}
}