/*
  Copyright 2019 www.dev5.cn, Inc. dev5@qq.com
 
  This file is part of X-MSG-IM.
 
  X-MSG-IM is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  X-MSG-IM is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.
 
  You should have received a copy of the GNU Affero General Public License
  along with X-MSG-IM.  If not, see <https://www.gnu.org/licenses/>.
 */

#ifndef XMSGIMAUTHTOKENCOLLOPER_H_
#define XMSGIMAUTHTOKENCOLLOPER_H_

#include <libx-msg-im-auth-core.h>

class XmsgImAuthTokenCollOper
{
public:
	bool load(void (*loadCb)(shared_ptr<XmsgImAuthTokenColl> coll)); 
	bool insert(shared_ptr<XmsgImAuthTokenColl> coll); 
	bool insert(void* conn, shared_ptr<XmsgImAuthTokenColl> coll); 
	bool delExpired(); 
	void job2deleteExpiredToken(ullong now); 
	static XmsgImAuthTokenCollOper* instance();
private:
	ullong lastDeleteExpiredTokenTs; 
	static XmsgImAuthTokenCollOper* inst;
	bool delExpired(void* conn); 
	shared_ptr<XmsgImAuthTokenColl> loadOneFromIter(void* it); 
	XmsgImAuthTokenCollOper();
	virtual ~XmsgImAuthTokenCollOper();
};

#endif 
