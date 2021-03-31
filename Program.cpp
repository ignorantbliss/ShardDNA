// Historian.cpp : This file contains the 'main' function. Program execution begins and ends there.
//

#include "pch.h"
#include <string>
#include <iostream>
#include "LogListener.h"
#include "Historian.h"
#include "History.h"
#include "ConfigFile.h"
#include "ShardCursor.h"
#include "QueryCursor.h"
#include <filesystem>

using namespace std;

int main(int argv, const char **args)
{   
	//Get the local path..	

	/*Historian Hist;
	Hist.ParseArguments(argv, args);

	if (!Hist.Init())
	{
		exit(-1);
	}*/
	History H;
	HistoryConfig HC;
	const char *p = args[0];
	std::filesystem::path Path(p);
	Path = Path.parent_path();
	HC.StorageFolder = Path.string();
	HC.StorageFolder += "\\Data";
	HC.TimePerShard = 60;
	
	H.Init(HC);

	//Hist.Log(LOGLEVEL_INFO, "Historian", "Intialisation Complete");
	time_t CurrentTime = time(NULL);
	/*for (int x = 0; x < 100; x++)
	{
		H.RecordValue("GOODBYE", x+1,CurrentTime + (time_t)x);
	}*/
	ShardCursor* Csr = H.GetHistory("HELLO", 0, CurrentTime);
	if (Csr == NULL)
	{
		cout << "ERROR: Unable to get history" << endl;
	}
	while ((Csr != NULL) && (Csr->Next() >= 0))
	{
		std::cout << Csr->Timestamp();
		std::cout << " " << Csr->Value() << endl;
	}
	delete Csr;

	Query Q;
	Q.from = 0;
	Q.to = -1;
	Q.MaxSamples = -1;
	Q.Channels.push_back("HELLO");
	Q.Channels.push_back("GOODBYE");
	Q.Options = 0;

	QueryCursor* C = H.GetHistory(Q);
	while (C->Next() != CURSOR_OK)
	{
		QueryRow QR = C->GetRow();
		cout << QR.ChannelID << "=" << QR.Value << " @ " << QR.Timestamp << endl;
	}
	delete C;	
	//std::cout << "Result: " << T.Value << endl;

	int rnd;
	cin >> rnd;
}


