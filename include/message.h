// message.h
#ifndef MESSAGE_H
#define MESSAGE_H

#include "connect.h"
#include "timer.h"
#include <websocketpp/config/asio_no_tls.hpp>
#include <websocketpp/server.hpp>
#include <string>
#include <map>
#include <json/json.h>

typedef websocketpp::server<websocketpp::config::asio> WsServer;

class ConnectionManager;

struct ValidationResult
{
    bool success;                               // 验证是否成功
    std::string code;                           // 错误代码
    std::string message;                        // 错误信息
    std::string webClientId;                    // Web 端 clientId
    std::string appClientId;                    // App 端 clientId
    Json::Value data;                           // json 数据
    bool valid;                                 // 是否有效
    std::map<std::string, std::string> details; // 详细信息
};

class MessageRouter
{
public:
    MessageRouter(ConnectionManager *connectionManager, WsServer *server);
    ~MessageRouter();

    // 验证消息格式
    // @param rawMessage - 原始消息
    // @return ValidationResult - 验证结果
    ValidationResult validate(const std::string &rawMessage);

    // 验证消息来源合法性
    // @param clientId - 消息中的 clientId
    // @param hdl - 当前连接句柄
    // @return bool - 是否合法
    bool validateSource(const std::string &clientId, websocketpp::connection_hdl hdl);

    // 处理绑定请求
    // @param data - 绑定请求数据
    // @param hdl - 当前 WebSocket 连接
    // @return object - 处理结果
    ValidationResult handleBind(std::string data, websocketpp::connection_hdl hdl);

    // 处理强度调节消息
    // @param data - 强度调节数据
    // @param hdl - 当前 WebSocket 连接
    // @return object - 处理结果
    ValidationResult handleStrengthAdjust(std::string data, websocketpp::connection_hdl hdl);

    // 转发网页端到APP
    // @param data - 网页端发送的数据
    // @param hdl - 当前 WebSocket 连接
    // @return object - 处理结果
    ValidationResult handleCustomStrength(std::string data, websocketpp::connection_hdl hdl);

    // 处理客户端消息（波形数据等）
    // @param data - 客户端消息数据
    // @param hdl - 当前 WebSocket 连接
    // @param timerManager - 定时器管理器
    // @return object - 处理结果
    ValidationResult handleClientMessage(std::string data, websocketpp::connection_hdl hdl, TimerManager *timerManager);

    // 通用消息转发
    // @param data - 消息数据
    // @param hdl - 当前 WebSocket 连接
    // @return object - 处理结果
    ValidationResult forwardMessage(std::string data, websocketpp::connection_hdl hdl);

private:
    ConnectionManager *connectionManager;
    WsServer *server;
};

#endif