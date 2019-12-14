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

typedef struct
{
	shared_ptr<XmsgImAuthTokenColl> coll;
	function<void(int ret, const string& desc)> cb;
} x_msg_im_auth_token; 

class XmsgImAuthTokenCollOper
{
public:
	bool load(void (*loadCb)(shared_ptr<XmsgImAuthTokenColl> coll)); 
	bool insert(shared_ptr<XmsgImAuthTokenColl> coll); 
	bool insert(void* conn, shared_ptr<XmsgImAuthTokenColl> coll); 
	void saveToken(shared_ptr<XmsgImAuthTokenColl> coll, function<void(int ret, const string& desc)> cb); 
	void loop(); 
	bool delExpired(); 
	void job2deleteExpiredToken(ullong now); 
	static XmsgImAuthTokenCollOper* instance();
private:
	ullong lastDeleteExpiredTokenTs; 
	queue<shared_ptr<x_msg_im_auth_token>> tokenQueue; 
	mutex lock4tokenQueue; 
	condition_variable cond4tokenQueue; 
	static XmsgImAuthTokenCollOper* inst;
	bool delExpired(void* conn); 
	shared_ptr<XmsgImAuthTokenColl> loadOneFromIter(void* it); 
	void insertBatch(const list<shared_ptr<x_msg_im_auth_token>>& lis, int& ret, string& desc); 
	XmsgImAuthTokenCollOper();
	virtual ~XmsgImAuthTokenCollOper();
};

#endif 
