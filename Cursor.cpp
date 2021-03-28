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

#include "pch.h"
#include "Cursor.h"
#include "BTree.h"
#include "History.h"

Cursor::Cursor()
{
	Active = NULL;
	Tree = NULL;
	Offset = 0;
	Index = 0;
	EndPoint = 0;
	Hist = NULL;

	EoF = false;
}

Cursor::~Cursor()
{	
	if (Active != NULL)
		delete Active;
}


void Cursor::SetPosition(std::streamoff offset, int index)
{	
	Offset = offset;
	Index = index;
}

void Cursor::SetTree(std::shared_ptr<BTree> Trx)
{
	Tree = Trx;
}

void Cursor::SetEndpoint(time_t ep)
{
	EndPoint = ep;
}



int Cursor::Next()
{
	//If we have reached the end, return false.
	if (this->EoF == true)
		return CURSOR_END;

	if (Active == NULL)
	{
		//First usage of this cursor. Load content.
		Active = new BTreeNode(Tree);
		Active->Load(Tree->DS.get(), Offset);
		if (Index == -1)
			Index = 0;
		return CURSOR_PRE;
	}
	else
	{
		Index++;

		//Check to see if we need the next node...
		if (Index >= Active->KeyCount())
		{			
			//Destroy the current node.
			time_t Current = *((time_t*)Active->Keys[Index-1]->Key());
			std::streamoff NextPos = Active->Next;
			delete Active;
			Active = NULL;

			Offset = NextPos;

			if (Offset != 0)
			{
				//Load the next node.
				Active = new BTreeNode(Tree);
				Active->Load(Tree->DS.get(), Offset);
				Index = 0;
			}
			else
			{
				EoF = true;				
			}
		}		
	}

	if (EoF == true)
		return CURSOR_END;

	time_t tm = *(time_t*)Active->Keys[Index]->Payload();
	if ((EndPoint > 0) && (tm > EndPoint))
	{
		EoF = true;
		return CURSOR_POST;
	}	
	return CURSOR_OK;
}

const void * Cursor::Key()
{
	if (Active == NULL)
	{
		return NULL;
	}
	return Active->Keys[Index]->Key();
}

const void * Cursor::Value()
{
	if (Active == NULL)
	{
		return NULL;
	}
	return Active->Keys[Index]->Payload();	
}

void Cursor::End()
{
	EoF = true;
}