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
#include "XmsgImAuthDb.h"
#include "XmsgImAuthAccountCollOper.h"
#include "XmsgImAuthCfgCollOper.h"
#include "XmsgImAuthTokenCollOper.h"

XmsgImAuthDb* XmsgImAuthDb::inst = new XmsgImAuthDb();
string XmsgImAuthDb::xmsgImAuthAccountColl = "tb_x_msg_im_auth_account"; 
string XmsgImAuthDb::xmsgImAuthTokenColl = "tb_x_msg_im_auth_token"; 

XmsgImAuthDb::XmsgImAuthDb()
{

}

XmsgImAuthDb* XmsgImAuthDb::instance()
{
	return XmsgImAuthDb::inst;
}

bool XmsgImAuthDb::load()
{
	auto& cfg = XmsgImAuthCfg::instance()->cfgPb->mysql();
	if (!MysqlConnPool::instance()->init(cfg.host(), cfg.port(), cfg.db(), cfg.usr(), cfg.password(), cfg.poolsize()))
		return false;
	LOG_INFO("init mysql connection pool successful, host: %s:%d, db: %s", cfg.host().c_str(), cfg.port(), cfg.db().c_str())
	if ("mysql" == XmsgImAuthCfg::instance()->cfgPb->cfgtype() && !this->initCfg())
		return false;
	ullong sts = DateMisc::dida();
	if (!XmsgImAuthAccountCollOper::instance()->load(XmsgImAuthAccountMgr::loadCb))
		return false;
	LOG_INFO("load %s.%s successful, count: %zu, elap: %dms", cfg.db().c_str(), XmsgImAuthDb::xmsgImAuthAccountColl.c_str(), XmsgImAuthAccountMgr::instance()->size(), DateMisc::elapDida(sts))
	sts = DateMisc::dida();
	if (!XmsgImAuthTokenCollOper::instance()->load(XmsgImAuthAccountTokenMgr::loadCb))
		return false;
	LOG_INFO("load %s.%s successful, count: %zu, elap: %dms", cfg.db().c_str(), XmsgImAuthDb::xmsgImAuthTokenColl.c_str(), XmsgImAuthAccountTokenMgr::instance()->size(), DateMisc::elapDida(sts))
	this->abst.reset(new ActorBlockingSingleThread("auth-db"));
	return true;
}

void XmsgImAuthDb::future(function<void()> cb)
{
	this->abst->future(cb);
}

bool XmsgImAuthDb::initCfg()
{
	shared_ptr<XmsgImAuthCfgColl> coll = XmsgImAuthCfgCollOper::instance()->load();
	if (coll == NULL)
		return false;
	LOG_INFO("got a x-msg-im-auth config from db: %s", coll->toString().c_str())
	XmsgImAuthCfg::instance()->cfgPb = coll->cfg;
	return true;
}

XmsgImAuthDb::~XmsgImAuthDb()
{

}

