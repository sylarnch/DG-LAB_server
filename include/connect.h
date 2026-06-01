// connect.h
#ifndef CONNECT_H
#define CONNECT_H

#include "message.h"
#include <websocketpp/config/asio_no_tls.hpp>
#include <websocketpp/server.hpp>

// 为方便使用，定义客户端类型别名
// typedef websocketpp::client<websocketpp::config::asio_client> client;
typedef websocketpp::server<websocketpp::config::asio> WsServer;

struct ValidationResult;

// 客户端类型
enum class ClientType
{
    UNKNOWN,
    WEB,
    APP
};

// 客户端信息结构体
struct ClientInfo
{
    // 客户端连接句柄
    websocketpp::connection_hdl hdl;
    // 客户端类型
    ClientType type;
    // 创建时间
    std::chrono::system_clock::time_point createdAt;
    // 最后心跳时间
    std::chrono::system_clock::time_point lastHeartbeat;
    // 是否存活
    bool isAlive;
};

class ConnectionManager
{

public:
    ConnectionManager(WsServer *server);
    ~ConnectionManager();

    // 注册新的连接
    // @param hdl - 连接句柄
    // @param clientId - 客户端 ID（已生成）
    // @param type - 连接类型 'web' 或 'app'
    void registerConn(websocketpp::connection_hdl hdl, std::string clientId, ClientType type = ClientType::UNKNOWN);

    // 断开连接并清理
    // @param clientId - 客户端 ID
    // @return ValidationResult 断开结果
    ValidationResult disconnect(std::string clientId);

    // 根据句柄获取id
    // @param hdl - 连接句柄
    // @return string 客户端 ID
    std::string getClientId(websocketpp::connection_hdl hdl);

    // 清理过期连接
    // @param timeout - 超时时间（秒）
    // @return int 清理的连接数
    int cleanupExpired(int timeout = 300);

    // 更新心跳时间
    // @param clientId - 客户端 ID
    void updateHeartbeat(std::string clientId);

    // 检查连接是否存在
    // @param clientId - 客户端 ID
    // @return bool 是否存在
    bool hasClient(std::string clientId);

    // 获取连接信息
    // @param clientId - 客户端 ID
    // @return bool 是否获取成功
    bool getClient(std::string clientId, ClientInfo &clientInfo);

    // 配对两个连接（web ↔ app）
    // @param webClientId - Web 端 clientId
    // @param appClientId - App 端 clientId
    // @return ValidationResult 配对结果
    ValidationResult pair(std::string webClientId, std::string appClientId);

    // 解除配对
    // @param clientId - 可以是 web 端或 app 端 clientId
    // @return ValidationResult 解除结果
    ValidationResult unpair(std::string clientId);

    // 获取配对的另一端
    // @param clientId - 发起方 clientId
    // @return string 配对的另一端 clientId
    std::string getPair(std::string clientId);

    // 检查配对关系是否有效
    // @param clientId - 发起方 clientId
    // @param targetId - 目标方 clientId
    // @return bool 是否配对
    bool isPaired(std::string clientId, std::string targetId);

    // 获取统计信息
    // @return map<string, string> 统计信息
    std::map<std::string, std::string> getStats();

    // 遍历所有客户端
    // @param callback - 接收 clientId 和 clientInfo 引用的回调函数
    void forEachClient(const std::function<void(const std::string &, const ClientInfo &)> &callback);

    // 清理所有连接
    void clear();

private:
    // 存储所有连接 clientId -> { hdl, type, createdAt, lastHeartbeat, isAlive }
    std::map<std::string, ClientInfo> connections;

    // 存储配对关系 webSocketId -> appSocketId
    std::map<std::string, std::string> pairings;

    // 反向映射 appSocketId -> webSocketId
    std::map<std::string, std::string> reversePairings;

    // 句柄->id映射
    std::map<websocketpp::connection_hdl, std::string, std::owner_less<websocketpp::connection_hdl>> hdlToId;

    // 服务器指针
    WsServer *server;

    // 互斥锁
    std::mutex mtx;
};

#endif // CONNECT_H