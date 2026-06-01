// timer.h
#ifndef TIMER_H
#define TIMER_H

#include <websocketpp/config/asio_no_tls.hpp>
#include <websocketpp/server.hpp>
#include <string>
#include <json/json.h>

typedef websocketpp::server<websocketpp::config::asio> WsServer;

struct TimerInfo
{
    std::string timerId;                   // 定时器ID
    int remaining;                         // 剩余时间或次数
    Json::Value message;                   // 消息内容
    websocketpp::connection_hdl targethdl; // 目标 WebSocket
    std::string channel;                   // 频道名
    websocketpp::connection_hdl sourcehdl; // 来源 WebSocket

    std::chrono::system_clock::time_point startedAt; // 开始时间
    std::string clientId;                            // 客户端 ID
    int timeSpace;                                   // 发送间隔（毫秒）
};

class TimerManager
{
public:
    TimerManager(WsServer *server);
    ~TimerManager();

    // 发送消息队列
    // @param clientId - 客户端 ID
    // @param channel - 频道名
    // @param targetHdl - 目标 WebSocket 连接
    // @param message - 消息内容
    // @param totalSends - 总发送次数
    // @param timeSpace - 发送间隔（毫秒）
    // @param sourceHdl - 消息来源 WebSocket（用于通知）
    void sendMessage(std::string clientId, std::string channel, websocketpp::connection_hdl targetHdl, Json::Value message, int totalSends, int timeSpace, websocketpp::connection_hdl sourceHdl);

    // 清除指定通道的所有定时器
    // @param clientId - 客户端 ID
    // @param channel - 频道名
    // @param sourceHdl - 来源 WebSocket（用于通知）
    void clearTimer(std::string clientId, std::string channel, websocketpp::connection_hdl sourceHdl);

    // 清除指定客户端的所有定时器
    // @param clientId - 客户端 ID
    void clearClientTimers(std::string clientId);

    // 获取定时器统计
    // @returns object - 定时器统计
    Json::Value getStats();

    // 清理所有定时器（用于服务关闭）
    void cleanupAll();

private:
    // 内部方法：启动发送消息
    // @private
    void _startSending(std::string clientId, std::string channel, websocketpp::connection_hdl targetHdl, Json::Value message, int totalSends, int timeSpace, websocketpp::connection_hdl sourceHdl);

    // 安全的发送消息
    // @private
    void _safeSend(websocketpp::connection_hdl hdl, Json::Value message);

    // clientId-channel -> TimerInfo
    std::map<std::string, TimerInfo> timers; // 定时器信息

    WsServer *server; // WebSocket 服务器实例
};
#endif