// server.h
#ifndef SERVER_H
#define SERVER_H
#include <websocketpp/server.hpp>
#include <websocketpp/config/asio_no_tls.hpp>
#include <string>
#include <map>
#include <json/json.h>
#include "connect.h"
#include "message.h"
#include "timer.h"
#include "tool.hpp"
#include "JSTimer.hpp"
#include <boost/asio/signal_set.hpp>

typedef websocketpp::server<websocketpp::config::asio> server;

class Server
{

public:
    Server();
    ~Server();

    // 启动服务
    // @param port - 服务端口
    void run(uint16_t port);

    // 优雅关闭
    void gracefulShutdown(int signal);

private:
    // 回调函数

    // 连接建立
    // @param hdl - 连接句柄
    void on_open(websocketpp::connection_hdl hdl);
    // 连接断开
    // @param hdl - 连接句柄
    void on_close(websocketpp::connection_hdl hdl);
    // 收到消息
    // @param hdl - 连接句柄
    // @param msg - 消息内容
    void on_message(websocketpp::connection_hdl hdl, server::message_ptr msg);
    // 连接失败
    // @param hdl - 连接句柄
    // 触发时机：在 WebSocket 握手完成之前发生的错误。比如端口被占用、URL 无效、网络无法连接、握手被拒绝等。
    void on_fail(websocketpp::connection_hdl hdl);

    // 处理绑定请求
    // @param data - 消息内容
    // @param hdl - 连接句柄
    void handleBind(Json::Value data, websocketpp::connection_hdl hdl);

    // 处理强度调节
    // @param data - 消息内容
    // @param hdl - 连接句柄
    void handleStrengthAdjust(Json::Value data, websocketpp::connection_hdl hdl);

    // 处理指定强度
    // @param data - 消息内容
    // @param hdl - 连接句柄
    void handleCustomStrength(Json::Value data, websocketpp::connection_hdl hdl);

    // 处理客户端消息（波形数据等）
    // @param data - 消息内容
    // @param hdl - 连接句柄
    void handleClientMessage(Json::Value data, websocketpp::connection_hdl hdl);

    // 处理心跳消息
    // @param data - 消息内容
    // @param hdl - 连接句柄
    void handleHeartbeat(Json::Value data, websocketpp::connection_hdl hdl);

    // 转发普通消息
    // @param data - 消息内容
    // @param hdl - 连接句柄
    void forwardMessage(Json::Value data, websocketpp::connection_hdl hdl);

    // 向所有客户端发送心跳
    void sendHeartbeats();

    // 服务器实例
    server m_server;
    // 连接对象
    ConnectionManager *m_connections;
    // 消息路由器对象
    MessageRouter *m_messageRouter;
    // 定时器管理器对象
    TimerManager *m_timerManager;

    // 心跳定时器
    std::string heartbeatInterval;

    // 创建随机数生成器对象（建议长期持有，不要每次构造）
    boost::uuids::random_generator gen;
};

#endif // SERVER_H