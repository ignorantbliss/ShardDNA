#include "pch.h"
#include "ConfigFile.h"
#include <string>
#include <fstream>

using namespace std;

ConfigFile::ConfigFile()
{
}


ConfigFile::~ConfigFile()
{
}

std::string ConfigFile::GetValueString(const char *st, const char *def)
{
	if (Values.find(st) == Values.end())
	{
		return def;
	}
	return Values[st];
}

int ConfigFile::GetValueInt(const char *st, int def)
{
	if (Values.find(st) == Values.end())
	{
		return def;
	}
	return stoi(Values[st]);
}

double ConfigFile::GetValueFloat(const char *st, double def)
{
	if (Values.find(st) == Values.end())
	{
		return def;
	}
	return stod(Values[st]);
}

bool ConfigFile::Load(const char *filename)
{
	ifstream reader;
	reader.open(filename, fstream::in);
	if (reader.is_open() == false)
	{
		return false;
	}
	string line;
	size_t delimiter;
	while (std::getline(reader, line))
	{
		delimiter = line.find('=');
		if (delimiter != string::npos)
		{
			string A = line.substr(0, delimiter);
			string B = line.substr(delimiter+1, line.length() - delimiter);
			Values[A] = B;
		}
	}
	return true;
}