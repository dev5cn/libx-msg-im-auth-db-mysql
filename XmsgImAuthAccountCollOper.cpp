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
#include "XmsgImAuthAccountCollOper.h"
#include "XmsgImAuthDb.h"

XmsgImAuthAccountCollOper* XmsgImAuthAccountCollOper::inst = new XmsgImAuthAccountCollOper();

XmsgImAuthAccountCollOper::XmsgImAuthAccountCollOper()
{

}

XmsgImAuthAccountCollOper* XmsgImAuthAccountCollOper::instance()
{
	return XmsgImAuthAccountCollOper::inst;
}

bool XmsgImAuthAccountCollOper::load(void (*loadCb)(shared_ptr<XmsgImAuthAccountColl> coll))
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool.")
		return false;
	}
	string sql;
	SPRINTF_STRING(&sql, "select * from %s", XmsgImAuthDb::xmsgImAuthAccountColl.c_str())
	bool ret = MysqlMisc::query(conn, sql, [loadCb](int ret, const string& desc, bool more, int rows, shared_ptr<MysqlResultRow> row)
	{
		if (ret != 0) 
		{
			LOG_ERROR("load %s failed, ret: %d, desc: %s", XmsgImAuthDb::xmsgImAuthAccountColl.c_str(), ret, desc.c_str())
			return false;
		}
		if (row == NULL) 
		{
			LOG_DEBUG("table %s no record", XmsgImAuthDb::xmsgImAuthAccountColl.c_str())
			return true;
		}
		auto coll = XmsgImAuthAccountCollOper::instance()->loadOneFromIter(row.get());
		if(coll == nullptr)
		{
			LOG_ERROR("have some one %s format error, row: %s", XmsgImAuthDb::xmsgImAuthAccountColl.c_str(), row->toString().c_str())
			return false; 
		}
		loadCb(coll);
		return true;
	});
	MysqlConnPool::instance()->relConn(conn, ret);
	return ret;
}

bool XmsgImAuthAccountCollOper::insert(shared_ptr<XmsgImAuthAccountColl> coll)
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

bool XmsgImAuthAccountCollOper::insert(void* conn, shared_ptr<XmsgImAuthAccountColl> coll)
{
	string sql;
	SPRINTF_STRING(&sql, "insert into %s values (?, ?, ?, ?, ?, ?, ?, ?)", XmsgImAuthDb::xmsgImAuthAccountColl.c_str())
	shared_ptr<MysqlCrudReq> req(new MysqlCrudReq(sql));
	req->addRow() 
	->addVarchar(coll->usr) 
	->addVarchar(coll->cgt->toString()) 
	->addVarchar(coll->pwdSha256) 
	->addBool(coll->localAuth) 
	->addBool(coll->enable) 
	->addBlob(coll->info->SerializeAsString()) 
	->addDateTime(coll->gts) 
	->addDateTime(coll->uts);
	return MysqlMisc::sql((MYSQL*) conn, req, [coll](int ret, const string& desc, int effected)
	{
		if (ret != 0)
		{
			LOG_ERROR("insert into %s.%s failed coll: %s, ret: %04X, error: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImAuthDb::xmsgImAuthAccountColl.c_str(), coll->toString().c_str(), ret, desc.c_str())
			return;
		}
		LOG_TRACE("insert into %s.%s successful, coll: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImAuthDb::xmsgImAuthAccountColl.c_str(), coll->toString().c_str())
	});
}

bool XmsgImAuthAccountCollOper::update(shared_ptr<XmsgImAuthAccountColl> coll)
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool.")
		return false;
	}
	string sql;
	SPRINTF_STRING(&sql, "update %s set usr = ?, pwdSha256 = ?, localAuth = ?, enable = ?, info = ?, uts = ? where cgt = ?", XmsgImAuthDb::xmsgImAuthAccountColl.c_str())
	shared_ptr<MysqlCrudReq> req(new MysqlCrudReq(sql));
	req->addRow() 
	->addVarchar(coll->usr) 
	->addVarchar(coll->pwdSha256) 
	->addBool(coll->localAuth) 
	->addBool(coll->enable) 
	->addBlob(coll->info->SerializeAsString()) 
	->addDateTime(coll->uts) 
	->addVarchar(coll->cgt->toString());
	ullong sts = DateMisc::dida();
	bool ret = MysqlMisc::sql((MYSQL*) conn, req, [coll, sts](int ret, const string& desc, int effected)
	{
		if (ret != 0)
		{
			LOG_ERROR("update table %s.%s failed, elap: %dms, coll: %s, ret: %04X, error: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImAuthDb::xmsgImAuthAccountColl.c_str(), DateMisc::elapDida(sts), coll->toString().c_str(), ret, desc.c_str())
			return;
		}
		LOG_DEBUG("update table %s.%s successful, elap: %dms, coll: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgImAuthDb::xmsgImAuthAccountColl.c_str(), DateMisc::elapDida(sts), coll->toString().c_str())
	});
	return ret;
}

shared_ptr<XmsgImAuthAccountColl> XmsgImAuthAccountCollOper::loadOneFromIter(void* it)
{
	MysqlResultRow* row = (MysqlResultRow*) it;
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
		LOG_ERROR("cgt format error, cgt: %s", str.c_str())
		return nullptr;
	}
	string pwdSha256;
	if (!row->getStr("pwdSha256", pwdSha256))
	{
		LOG_ERROR("can not found field: pwdSha256")
		return nullptr;
	}
	bool localAuth;
	if (!row->getBool("localAuth", localAuth))
	{
		LOG_ERROR("can not found field: localAuth")
		return nullptr;
	}
	bool enable;
	if (!row->getBool("enable", enable))
	{
		LOG_ERROR("can not found field: enable")
		return nullptr;
	}
	if (!row->getBin("info", str))
	{
		LOG_ERROR("can not found field: info")
		return nullptr;
	}
	shared_ptr<XmsgKv> info(new XmsgKv());
	if (!info->ParseFromString(str))
	{
		LOG_ERROR("info format error")
		return nullptr;
	}
	ullong gts;
	if (!row->getLong("gts", gts))
	{
		LOG_ERROR("can not found field: gts")
		return nullptr;
	}
	ullong uts;
	if (!row->getLong("uts", uts))
	{
		LOG_ERROR("can not found field: uts")
		return nullptr;
	}
	shared_ptr<XmsgImAuthAccountColl> coll(new XmsgImAuthAccountColl());
	coll->usr = usr;
	coll->cgt = cgt;
	coll->pwdSha256 = pwdSha256;
	coll->localAuth = localAuth;
	coll->enable = enable;
	coll->info = info;
	coll->gts = gts;
	coll->uts = uts;
	return coll;
}

XmsgImAuthAccountCollOper::~XmsgImAuthAccountCollOper()
{

}

