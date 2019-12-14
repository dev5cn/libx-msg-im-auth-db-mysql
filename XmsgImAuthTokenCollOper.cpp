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

#include <libmisc-mysql-c.h>
#include "XmsgImAuthTokenCollOper.h"
#include "XmsgImAuthDb.h"

XmsgImAuthTokenCollOper* XmsgImAuthTokenCollOper::inst = new XmsgImAuthTokenCollOper();

XmsgImAuthTokenCollOper::XmsgImAuthTokenCollOper()
{
	this->lastDeleteExpiredTokenTs = DateMisc::nowGmt0();
}

XmsgImAuthTokenCollOper* XmsgImAuthTokenCollOper::instance()
{
	return XmsgImAuthTokenCollOper::inst;
}

bool XmsgImAuthTokenCollOper::load(void (*loadCb)(shared_ptr<XmsgImAuthTokenColl> coll))
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool.")
		return false;
	}
	if (!this->delExpired(conn))
		return false;
	string sql;
	SPRINTF_STRING(&sql, "select * from %s", XmsgImAuthDb::xmsgImAuthTokenColl.c_str())
	bool ret = MysqlMisc::query(conn, sql, [loadCb](int ret, const string& desc, bool more, int rows, shared_ptr<MysqlResultRow> row)
	{
		if (ret != 0) 
		{
			LOG_ERROR("load %s failed, ret: %d, desc: %s", XmsgImAuthDb::xmsgImAuthTokenColl.c_str(), ret, desc.c_str())
			return false;
		}
		if (row == NULL) 
		{
			LOG_DEBUG("table %s no record", XmsgImAuthDb::xmsgImAuthTokenColl.c_str())
			return true;
		}
		auto coll = XmsgImAuthTokenCollOper::instance()->loadOneFromIter(row.get());
		if(coll == nullptr)
		{
			LOG_ERROR("have some one %s format error, row: %s", XmsgImAuthDb::xmsgImAuthTokenColl.c_str(), row->toString().c_str())
			return false; 
		}
		LOG_RECORD("got a %s: %s", XmsgImAuthDb::xmsgImAuthTokenColl.c_str(), coll->toString().c_str())
		loadCb(coll);
		return true;
	});
	MysqlConnPool::instance()->relConn(conn, ret);
	return ret;
}

bool XmsgImAuthTokenCollOper::insert(shared_ptr<XmsgImAuthTokenColl> coll)
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool.")
		return false;
	}
	bool ret = this->insert(conn, coll);
	MysqlConnPool::instance()->relConn(conn, ret);
	return ret;
}

bool XmsgImAuthTokenCollOper::insert(void* conn, shared_ptr<XmsgImAuthTokenColl> coll)
{
	string sql;
	SPRINTF_STRING(&sql, "insert into %s values (?, ?, ?, ?, ?, ?, ?)", XmsgImAuthDb::xmsgImAuthTokenColl.c_str())
	shared_ptr<MysqlCrudReq> req(new MysqlCrudReq(sql));
	req->addRow() 
	->addVarchar(coll->token) 
	->addVarchar(coll->usr) 
	->addVarchar(coll->cgt->toString()) 
	->addVarchar(coll->secret) 
	->addDateTime(coll->gts) 
	->addDateTime(coll->expired) 
	->addBlob(coll->info->SerializeAsString());
	return MysqlMisc::sql((MYSQL*) conn, req, [coll](int ret, const string& desc, int effected)
	{
		if (ret != 0)
		{
			LOG_ERROR("insert into %s.%s failed, coll: %s, ret: %04X, error: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImAuthDb::xmsgImAuthTokenColl.c_str(), coll->toString().c_str(), ret, desc.c_str())
			return;
		}
		LOG_TRACE("insert into %s.%s successful, coll: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImAuthDb::xmsgImAuthTokenColl.c_str(), coll->toString().c_str())
	});
}

void XmsgImAuthTokenCollOper::saveToken(shared_ptr<XmsgImAuthTokenColl> coll, function<void(int ret, const string& desc)> cb)
{
	shared_ptr<x_msg_im_auth_token> xmiat(new x_msg_im_auth_token());
	xmiat->coll = coll;
	xmiat->cb = cb;
	unique_lock<mutex> lock(this->lock4tokenQueue);
	bool nll = this->tokenQueue.empty();
	this->tokenQueue.push(xmiat);
	if (nll)
		this->cond4tokenQueue.notify_one(); 
}

void XmsgImAuthTokenCollOper::loop()
{
	list<shared_ptr<x_msg_im_auth_token>> lis;
	unique_lock<mutex> lock(this->lock4tokenQueue);
	while (!this->tokenQueue.empty())
	{
		auto xmiat = this->tokenQueue.front();
		this->tokenQueue.pop();
		lis.push_back(xmiat);
		if (lis.size() < XmsgImAuthCfg::instance()->cfgPb->misc().tokensavebatchsize())
			continue;
		break;
	}
	if (lis.empty())
	{
		this->cond4tokenQueue.wait(lock);
		return;
	}
	lock.unlock();
	ullong sts = DateMisc::dida();
	int ret = 0;
	string desc;
	this->insertBatch(lis, ret, desc);
	for (auto& it : lis)
		it->cb(ret, desc);
	LOG_DEBUG("insert batch size: %zu, elap: %dms, ret: %d, desc: %s", lis.size(), DateMisc::elapDida(sts), ret, desc.c_str())
}

void XmsgImAuthTokenCollOper::insertBatch(const list<shared_ptr<x_msg_im_auth_token>>& lis, int& ret, string& desc)
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		ret = RET_EXCEPTION;
		desc = "can not get connection from pool";
		LOG_ERROR("can not get connection from pool, size: %zu", lis.size())
		return;
	}
	if (!MysqlMisc::start(conn))
	{
		ret = RET_EXCEPTION;
		desc = "start transaction failed";
		LOG_ERROR("start transaction failed, err: %s, size: %zu", ::mysql_error(conn), lis.size())
		MysqlConnPool::instance()->relConn(conn, false);
		return;
	}
	string sql;
	SPRINTF_STRING(&sql, "insert into %s values (?, ?, ?, ?, ?, ?, ?)", XmsgImAuthDb::xmsgImAuthTokenColl.c_str())
	shared_ptr<MysqlCrudReq> req(new MysqlCrudReq(sql));
	for (auto& it : lis)
	{
		req->addRow() 
		->addVarchar(it->coll->token) 
		->addVarchar(it->coll->usr) 
		->addVarchar(it->coll->cgt->toString()) 
		->addVarchar(it->coll->secret) 
		->addDateTime(it->coll->gts) 
		->addDateTime(it->coll->expired) 
		->addBlob(it->coll->info->SerializeAsString());
	}
	int affected = 0;
	if (!MysqlMisc::sql(conn, req, ret, desc, &affected))
	{
		ret = RET_EXCEPTION;
		desc = "insert batch failed";
		LOG_ERROR("insert many into %s failed, err: %s, size: %zu", XmsgImAuthDb::xmsgImAuthTokenColl.c_str(), desc.c_str(), lis.size())
		MysqlMisc::rollBack(conn);
		MysqlConnPool::instance()->relConn(conn, false);
		return;
	}
	if (affected != (int) lis.size())
	{
		LOG_FAULT("it`s a bug, affected: %d, size: %zu", affected, lis.size())
	}
	if (!MysqlMisc::commit(conn))
	{
		ret = RET_EXCEPTION;
		desc = "commit transaction failed";
		LOG_ERROR("commit transaction failed, err: %s, size: %zu", ::mysql_error(conn), lis.size())
		MysqlMisc::rollBack(conn);
		MysqlConnPool::instance()->relConn(conn, false);
		return;
	}
	MysqlConnPool::instance()->relConn(conn);
}

bool XmsgImAuthTokenCollOper::delExpired()
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool.")
		return false;
	}
	bool ret = this->delExpired(conn);
	MysqlConnPool::instance()->relConn(conn, ret);
	return ret;
}

bool XmsgImAuthTokenCollOper::delExpired(void* conn)
{
	string sql;
	SPRINTF_STRING(&sql, "delete from %s where expired < '%s'", XmsgImAuthDb::xmsgImAuthTokenColl.c_str(), DateMisc::to_yyyy_mm_dd_hh_mi_ss(DateMisc::nowGmt0() / 1000L).c_str())
	ullong sts = DateMisc::dida();
	return MysqlMisc::sql((MYSQL*) conn, sql, [sql, sts](int ret, const string& desc, int effected)
	{
		if (ret != 0)
		{
			LOG_ERROR("execute sql failed, elap: %dms, sql: %s, ret: %04X, error: %s", DateMisc::elapDida(sts), sql.c_str(), ret, desc.c_str())
			return;
		}
		LOG_TRACE("execute sql successful, elap: %dms, deleted-size: %d, sql: %s", DateMisc::elapDida(sts), effected, sql.c_str())
	});
}

void XmsgImAuthTokenCollOper::job2deleteExpiredToken(ullong now)
{
	if (now - this->lastDeleteExpiredTokenTs < (ullong) DateMisc::minute * 5)
		return;
	this->lastDeleteExpiredTokenTs = now;
	this->delExpired();
}

shared_ptr<XmsgImAuthTokenColl> XmsgImAuthTokenCollOper::loadOneFromIter(void* it)
{
	MysqlResultRow* row = (MysqlResultRow*) it;
	string token;
	if (!row->getStr("token", token))
	{
		LOG_ERROR("can not found field: token")
		return nullptr;
	}
	string usr;
	if (!row->getStr("usr", usr))
	{
		LOG_ERROR("can not found field: usr")
		return nullptr;
	}
	string str;
	if (!row->getStr("cgt", str))
	{
		LOG_ERROR("can not found field: cgt")
		return nullptr;
	}
	SptrCgt cgt = ChannelGlobalTitle::parse(str);
	if (cgt == nullptr)
	{
		LOG_ERROR("cgt format error, usr: %s, cgt: %s", usr.c_str(), str.c_str())
		return nullptr;
	}
	string secret;
	if (!row->getStr("secret", secret))
	{
		LOG_ERROR("can not found field: secret")
		return nullptr;
	}
	ullong gts;
	if (!row->getLong("gts", gts))
	{
		LOG_ERROR("can not found field: gts")
		return nullptr;
	}
	ullong expired;
	if (!row->getLong("expired", expired))
	{
		LOG_ERROR("can not found field: expired")
		return nullptr;
	}
	if (!row->getBin("info", str))
	{
		LOG_ERROR("can not found field: info")
		return nullptr;
	}
	shared_ptr<XmsgImClientDeviceInfo> info(new XmsgImClientDeviceInfo());
	if (!info->ParseFromString(str))
	{
		LOG_ERROR("info format error")
		return nullptr;
	}
	shared_ptr<XmsgImAuthTokenColl> coll(new XmsgImAuthTokenColl());
	coll->token = token;
	coll->usr = usr;
	coll->cgt = cgt;
	coll->secret = secret;
	coll->gts = gts;
	coll->expired = expired;
	coll->info = info;
	return coll;
}

XmsgImAuthTokenCollOper::~XmsgImAuthTokenCollOper()
{

}

