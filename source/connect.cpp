// connect.cpp

#include "connect.h"

ConnectionManager::ConnectionManager(WsServer *server)
{
    this->server = server;
}

ConnectionManager::~ConnectionManager()
{
    // 关闭所有连接
    clear();
    std::cout << "连接管理器已关闭！" << std::endl;
}

void ConnectionManager::registerConn(websocketpp::connection_hdl hdl, std::string clientId, ClientType type)
{
    ClientInfo info;
    info.hdl = hdl;
    info.type = type;
    info.createdAt = std::chrono::system_clock::now();
    info.lastHeartbeat = std::chrono::system_clock::now();
    info.isAlive = true;

    // 加锁
    std::lock_guard<std::mutex> lock(this->mtx);
    this->connections.insert(std::make_pair(clientId, info));
    this->hdlToId.insert(std::make_pair(hdl, clientId));
}

std::string ConnectionManager::getClientId(websocketpp::connection_hdl hdl)
{
    auto it = this->hdlToId.find(hdl);
    if (it != this->hdlToId.end())
    {
        return it->second;
    }
    return "";
}

bool ConnectionManager::getClient(std::string clientId, ClientInfo &clientInfo)
{
    if (!hasClient(clientId))
    {
        std::cout << "获取连接信息失败！连接不存在！" << std::endl;
        return false;
    }
    clientInfo = this->connections.at(clientId);
    return true;
}

bool ConnectionManager::hasClient(std::string clientId)
{
    auto it = this->connections.find(clientId);
    if (it != this->connections.end())
    {
        return true;
    }
    return false;
}

ValidationResult ConnectionManager::pair(std::string webClientId, std::string appClientId)
{
    ValidationResult result;
    // 检查双方是否都已连接
    if (!hasClient(webClientId) || !hasClient(appClientId))
    {
        std::cout << "配对失败！客户端未连接！" << std::endl;
        result.success = false;
        result.code = "401";
        result.message = "客户端未连接";
        return result;
    }
    // 检查是否任何一方已有配对
    if (this->pairings.find(webClientId) != this->pairings.end() || this->reversePairings.find(appClientId) != this->reversePairings.end())
    {
        std::cout << "配对失败！客户端已被配对！" << std::endl;
        result.success = false;
        result.code = "400";
        result.message = "客户端已被配对，请先解除当前配对";
        return result;
    }

    // 加锁
    std::lock_guard<std::mutex> lock(this->mtx);
    // 创建配对关系
    this->pairings.insert(std::make_pair(webClientId, appClientId));
    this->reversePairings.insert(std::make_pair(appClientId, webClientId));

    std::cout << "配对成功！" << std::endl;
    result.success = true;
    result.code = "200";
    result.message = "配对成功";
    result.webClientId = webClientId;
    result.appClientId = appClientId;
    return result;
}

ValidationResult ConnectionManager::unpair(std::string clientId)
{
    ValidationResult result;
    // 判断客户端是否连接
    if (!hasClient(clientId))
    {
        std::cout << "解除配对失败！客户端未连接！" << std::endl;
        result.success = false;
        result.code = "401";
        result.message = "客户端未连接";
        return result;
    }

    // 找到两个客户端的 ID
    std::string webClientId = "";
    std::string appClientId = "";
    if (this->pairings.find(clientId) != this->pairings.end())
    {
        // 是 web 端
        webClientId = clientId;
        appClientId = this->pairings.at(clientId);
    }
    else if (this->reversePairings.find(clientId) != this->reversePairings.end())
    {
        // 是 app 端
        webClientId = this->reversePairings.at(clientId);
        appClientId = clientId;
    }
    else
    {
        // 未找到配对关系
        std::cout << "解除配对失败！客户端未配对！" << std::endl;
        result.success = false;
        result.code = "404";
        result.message = "未找到配对关系";
        return result;
    }

    // 加锁
    std::lock_guard<std::mutex> lock(this->mtx);
    // 解除配对
    this->pairings.erase(webClientId);
    this->reversePairings.erase(appClientId);
    std::cout << "解除配对成功！" << std::endl;
    result.success = true;
    result.code = "200";
    result.message = "配对已解除";
    return result;
}

std::string ConnectionManager::getPair(std::string clientId)
{
    // 判断是什么客户端
    if (this->pairings.find(clientId) != this->pairings.end())
    {
        // 是 web 端
        return this->pairings.at(clientId);
    }
    else if (this->reversePairings.find(clientId) != this->reversePairings.end())
    {
        // 是 app 端
        return this->reversePairings.at(clientId);
    }
    else
    {
        std::cout << "获取配对失败！客户端未配对！" << std::endl;
        return "";
    }
}

bool ConnectionManager::isPaired(std::string clientId, std::string targetId)
{
    // 判断客户端是否连接
    if (!hasClient(clientId) || !hasClient(targetId))
    {
        std::cout << "检查配对关系失败！客户端未连接！" << std::endl;
        return false;
    }

    // 判断是否配对
    if (this->pairings.find(clientId) != this->pairings.end() && this->pairings.at(clientId) == targetId)
    {
        return true;
    }
    else if (this->reversePairings.find(clientId) != this->reversePairings.end() && this->reversePairings.at(clientId) == targetId)
    {
        return true;
    }
    else
    {
        return false;
    }
}

ValidationResult ConnectionManager::disconnect(std::string clientId)
{
    ValidationResult result;
    // 判断客户端是否连接
    if (!hasClient(clientId))
    {
        std::cout << "断开连接失败！客户端未连接！" << std::endl;
        result.success = false;
        result.code = "404";
        return result;
    }

    // 如果是配对状态，先解除配对
    std::string pairId = getPair(clientId);
    if (!pairId.empty())
    {
        std::cout << clientId << "断开，关联配对：" << pairId << std::endl;

        // 通知配对的另一端
        websocketpp::connection_hdl pairHdl = connections.at(pairId).hdl;
        auto con = server->get_con_from_hdl(pairHdl);
        if (con && con->get_state() == websocketpp::session::state::open)
        {
            Json::Value jsonMsg;
            jsonMsg["type"] = "break";
            jsonMsg["clientId"] = clientId;
            jsonMsg["targetId"] = pairId;
            jsonMsg["message"] = "209";
            std::string strMsg = Json::FastWriter().write(jsonMsg);
            try
            {
                server->send(pairHdl, strMsg, websocketpp::frame::opcode::text);
            }
            catch (const websocketpp::exception &e)
            {
                std::cerr << "发送断开通知失败：" << e.what() << '\n';
            }
            catch (const std::exception &e)
            {
                std::cerr << "发送断开通知失败：" << e.what() << '\n';
            }
        }

        // 解除配对
        unpair(clientId);
    }

    // 加锁
    std::lock_guard<std::mutex> lock(this->mtx);
    try
    {
        auto con = server->get_con_from_hdl(this->connections.at(clientId).hdl);
        if (con && con->get_state() == websocketpp::session::state::open)
        {
            // 关闭当前连接
            this->server->close(this->connections.at(clientId).hdl, websocketpp::close::status::normal, "Client Disconnect");
        }
    }
    catch (const websocketpp::exception &e)
    {
    }
    catch (const std::exception &e)
    {
    }

    // 从存储中删除连接
    this->hdlToId.erase(connections.at(clientId).hdl);
    this->connections.erase(clientId);
    std::cout << "断开 " << clientId << " 成功！剩余连接数：" << this->connections.size() << std::endl;

    result.success = true;
    result.code = "200";
    return result;
}
void ConnectionManager::updateHeartbeat(std::string clientId)
{
    // 判断客户端是否连接
    if (!hasClient(clientId))
    {
        std::cout << "更新心跳失败！客户端未连接！" << std::endl;
        return;
    }

    // 加锁
    std::lock_guard<std::mutex> lock(this->mtx);
    // 更新心跳时间
    this->connections.at(clientId).lastHeartbeat = std::chrono::system_clock::now();
    this->connections.at(clientId).isAlive = true;
}

std::map<std::string, std::string> ConnectionManager::getStats()
{
    // 加锁
    std::lock_guard<std::mutex> lock(this->mtx);
    // 获取统计信息
    std::map<std::string, std::string> stats;
    stats["totalConnections"] = std::to_string(this->connections.size());
    stats["pairedConnections"] = std::to_string(this->pairings.size());
    stats["unpairedConnections"] = std::to_string(this->connections.size() - this->pairings.size() * 2);
    return stats;
}

int ConnectionManager::cleanupExpired(int timeout)
{
    // 清理过期连接
    int expiredCount = 0;                              // 过期连接数
    const auto now = std::chrono::system_clock::now(); // 当前时间
    for (auto it = this->connections.begin(); it != this->connections.end(); it++)
    {
        const auto age = std::chrono::duration_cast<std::chrono::seconds>(now - it->second.lastHeartbeat).count();
        if (age > timeout)
        {
            // 关闭连接
            expiredCount++;
            std::cout << "关闭 " << it->first << " 连接，已过期 " << age << " 秒" << std::endl;
            this->disconnect(it->first);
        }
    }
    return expiredCount;
}

void ConnectionManager::forEachClient(const std::function<void(const std::string &, const ClientInfo &)> &callback)
{
    for (const auto &[clientId, clientInfo] : connections)
    {
        callback(clientId, clientInfo);
    }
}

void ConnectionManager::clear()
{
    for (auto it : this->connections)
    {
        disconnect(it.first);
    }
}
