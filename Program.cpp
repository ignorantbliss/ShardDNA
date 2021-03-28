// Historian.cpp : This file contains the 'main' function. Program execution begins and ends there.
//

#include "pch.h"
#include <string>
#include <iostream>
#include "LogListener.h"
#include "Historian.h"
#include "ConfigFile.h"
#include "ShardCursor.h"

using namespace std;

int main(int argv, const char **args)
{   
	//Get the local path..	

	Historian Hist;
	Hist.ParseArguments(argv, args);

	if (!Hist.Init())
	{
		exit(-1);
	}
	Hist.Log(LOGLEVEL_INFO, "Historian", "Intialisation Complete");
	time_t CurrentTime = time(NULL);
	for (int x = 0; x < 100; x++)
	{
		//Hist.Data.RecordValue("HELLO", x+1,CurrentTime + (time_t)x);
	}
	//TSPoint T = Hist.GetLatest("HELLO");
	ShardCursor* Csr = Hist.Data.GetHistory("HELLO", 0, CurrentTime);
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
	//std::cout << "Result: " << T.Value << endl;

	int rnd;
	cin >> rnd;
}


