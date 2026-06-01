// message.cpp
#include "message.h"
#include "tool.hpp"
#include <websocketpp/error.hpp>

MessageRouter::MessageRouter(ConnectionManager *connectionManager, WsServer *server)
{
    this->connectionManager = connectionManager;
    this->server = server;
}
MessageRouter::~MessageRouter()
{
}

ValidationResult MessageRouter::validate(const std::string &rawMessage)
{
    ValidationResult result;
    Json::Reader reader;

    // 解析 JSON
    try
    {
        reader.parse(rawMessage, result.data);
    }
    catch (const std::exception &e)
    {
        result.valid = false;
        result.code = "403";
        result.message = "消息格式错误：期望 JSON 格式";
        return result;
    }

    // 所有消息必须包含 type, clientId, targetId, message
    if (!result.data.isMember("type") || !result.data.isMember("clientId") || !result.data.isMember("message") || !result.data.isMember("targetId"))
    {
        result.valid = false;
        result.code = "404";
        result.message = "消息缺少必需字段";
        return result;
    }

    result.valid = true;
    return result;
}

bool MessageRouter::validateSource(const std::string &clientId, websocketpp::connection_hdl hdl)
{
    std::string hdlClientId = this->connectionManager->getClientId(hdl);
    return hdlClientId == clientId;
}

ValidationResult MessageRouter::handleBind(std::string data, websocketpp::connection_hdl hdl)
{
    Json::Reader reader;
    Json::Value parsedData;
    reader.parse(data, parsedData);
    std::string clientId = parsedData["clientId"].asString();
    std::string targetId = parsedData["targetId"].asString();

    ValidationResult result;

    // 验证消息来源
    if (!this->validateSource(clientId, hdl) && !this->validateSource(targetId, hdl))
    {
        result.success = false;
        result.code = "404";
        result.message = "非法消息来源";
        return result;
    }

    // 执行配对
    result = this->connectionManager->pair(clientId, targetId);

    if (result.success)
    {
        // 向双方发送绑定成功消息
        Json::Value bindMessage;
        bindMessage["type"] = "bind";
        bindMessage["clientId"] = clientId;
        bindMessage["targetId"] = targetId;
        bindMessage["message"] = "200";
        Json::FastWriter writer;
        std::string bindMsg = writer.write(bindMessage);

        // 发送给发起方（APP）
        try
        {
            this->server->send(hdl, bindMsg, websocketpp::frame::opcode::text);
        }
        catch (const websocketpp::exception &e)
        {
            std::cout << "【websocket异常】发送绑定消息失败：" << e.what() << std::endl;
        }
        catch (const std::exception &e)
        {
            std::cout << "【标准异常】发送绑定消息失败：" << e.what() << std::endl;
        }

        // 发送给另一方（网页端）— clientId 是网页端 ID
        ClientInfo otherClient;
        bool res = this->connectionManager->getClient(clientId, otherClient);
        if (res && server->get_con_from_hdl(otherClient.hdl) && server->get_con_from_hdl(otherClient.hdl)->get_state() == websocketpp::session::state::open && connectionManager->getClientId(hdl) != clientId)
        {
            try
            {
                this->server->send(otherClient.hdl, bindMsg, websocketpp::frame::opcode::text);
            }
            catch (const websocketpp::exception &e)
            {
                std::cout << "【websocket异常】发送绑定消息失败：" << e.what() << std::endl;
            }
            catch (const std::exception &e)
            {
                std::cout << "【标准异常】发送绑定消息失败：" << e.what() << std::endl;
            }
        }
    }
    return result;
}

ValidationResult MessageRouter::handleStrengthAdjust(std::string data, websocketpp::connection_hdl hdl)
{
    Json::Reader reader;
    Json::Value parsedData;
    reader.parse(data, parsedData);
    std::string clientId = parsedData["clientId"].asString();
    std::string targetId = parsedData["targetId"].asString();
    std::string type = parsedData["type"].asString();
    std::string channel = parsedData["channel"].asString();
    std::string strength = parsedData["strength"].asString();

    ValidationResult result;

    // 验证配对关系
    if (!this->connectionManager->isPaired(clientId, targetId))
    {
        result.success = false;
        result.code = "402";
        result.message = "配对关系无效";
        return result;
    }

    // 构造强度调整消息（type 1,2,3 对应减少、增加、指定强度）
    int sendType = std::stoi(type) - 1;
    int sendChannel = channel.empty() ? 1 : std::stoi(channel);
    int sendStrength = std::stoi(type) >= 3 ? (strength.empty() ? 0 : std::stoi(strength)) : 1;

    const std::string strengthMessage = "strength-" + std::to_string(sendChannel) + "+" + std::to_string(sendType) + "+" + std::to_string(sendStrength);

    ClientInfo targetClient;
    bool res = this->connectionManager->getClient(targetId, targetClient);
    if (res && server->get_con_from_hdl(targetClient.hdl) && server->get_con_from_hdl(targetClient.hdl)->get_state() == websocketpp::session::state::open)
    {
        try
        {
            Json::Value message;
            message["type"] = "msg";
            message["clientId"] = clientId;
            message["targetId"] = targetId;
            message["message"] = strengthMessage;
            Json::FastWriter writer;
            std::string msg = writer.write(message);
            this->server->send(targetClient.hdl, msg, websocketpp::frame::opcode::text);
            std::cout << "强度调整：" << strengthMessage << std::endl;
        }
        catch (const websocketpp::exception &e)
        {
            std::cerr << "【websocket异常】发送强度消息失败：" << e.what() << std::endl;
            result.success = false;
            result.code = "500";
            result.message = "发送失败";
            return result;
        }
        catch (const std::exception &e)
        {
            std::cerr << "【标准异常】发送强度消息失败：" << e.what() << std::endl;
            result.success = false;
            result.code = "500";
            result.message = "发送失败";
            return result;
        }
        result.success = true;
        result.code = "200";
        result.message = "强度调整已发送";
    }
    return result;
}

ValidationResult MessageRouter::handleCustomStrength(std::string data, websocketpp::connection_hdl hdl)
{
    Json::Reader reader;
    Json::Value parsedData;
    reader.parse(data, parsedData);
    std::string clientId = parsedData["clientId"].asString();
    std::string targetId = parsedData["targetId"].asString();
    std::string message = parsedData["message"].asString();

    ValidationResult result;
    // 验证配对关系
    if (!this->connectionManager->isPaired(clientId, targetId))
    {
        result.success = false;
        result.code = "402";
        result.message = "配对关系无效";
        return result;
    }

    // 转发网页端到APP
    ClientInfo targetClient;
    bool res = this->connectionManager->getClient(targetId, targetClient);
    if (res && server->get_con_from_hdl(targetClient.hdl) && server->get_con_from_hdl(targetClient.hdl)->get_state() == websocketpp::session::state::open)
    {
        try
        {
            Json::Value customMessage;
            customMessage["type"] = "msg";
            customMessage["clientId"] = clientId;
            customMessage["targetId"] = targetId;
            customMessage["message"] = message;
            Json::FastWriter writer;
            std::string customMsg = writer.write(customMessage);
            this->server->send(targetClient.hdl, customMsg, websocketpp::frame::opcode::text);
            std::cout << "转发网页端到APP：" << message << std::endl;
        }
        catch (const websocketpp::exception &e)
        {
            std::cerr << "【websocket异常】转发网页端到APP失败：" << e.what() << std::endl;
            result.success = false;
            result.code = "500";
            result.message = "发送失败";
            return result;
        }
        catch (const std::exception &e)
        {
            std::cerr << "【标准异常】转发网页端到APP失败：" << e.what() << std::endl;
            result.success = false;
            result.code = "500";
            result.message = "发送失败";
            return result;
        }

        result.success = true;
        result.code = "200";
        result.message = "转发网页端到APP成功";
    }
    return result;
}

ValidationResult MessageRouter::forwardMessage(std::string data, websocketpp::connection_hdl hdl)
{
    Json::Reader reader;
    Json::Value parsedData;
    reader.parse(data, parsedData);
    std::string clientId = parsedData["clientId"].asString();
    std::string targetId = parsedData["targetId"].asString();
    std::string type = parsedData["type"].asString();
    std::string message = parsedData["message"].asString();

    ValidationResult result;

    // 验证配对关系
    if (!this->connectionManager->isPaired(clientId, targetId))
    {
        result.success = false;
        result.code = "402";
        result.message = "配对关系无效";
        return result;
    }

    // v1兼容：默认消息转发给 clientId（网页端）
    // 场景：APP 收到强度下发后，回复的消息需要转发回网页端
    ClientInfo client;
    bool res = this->connectionManager->getClient(clientId, client);
    if (res && server->get_con_from_hdl(client.hdl) && server->get_con_from_hdl(client.hdl)->get_state() == websocketpp::session::state::open)
    {
        try
        {
            Json::Value forwardMessage;
            forwardMessage["type"] = type;
            forwardMessage["clientId"] = clientId;
            forwardMessage["targetId"] = targetId;
            forwardMessage["message"] = message;
            Json::FastWriter writer;
            std::string forwardMsg = writer.write(forwardMessage);
            this->server->send(client.hdl, forwardMsg, websocketpp::frame::opcode::text);
            std::cout << "消息转发：" << forwardMsg << std::endl;
        }
        catch (const websocketpp::exception &e)
        {
            std::cerr << "【websocket异常】消息转发失败：" << e.what() << std::endl;
            result.success = false;
            result.code = "500";
            result.message = "发送失败";
            return result;
        }
        catch (const std::exception &e)
        {
            std::cerr << "【标准异常】消息转发失败：" << e.what() << std::endl;
            result.success = false;
            result.code = "500";
            result.message = "发送失败";
            return result;
        }
    }
    else
    {
        result.success = false;
        result.code = "404";
        result.message = "客户端不存在";
        return result;
    }

    result.success = true;
    result.code = "200";
    result.message = "消息已转发";
    return result;
}

ValidationResult MessageRouter::handleClientMessage(std::string data, websocketpp::connection_hdl hdl, TimerManager *timerManager)
{
    Json::Reader reader;
    Json::Value parsedData;
    reader.parse(data, parsedData);
    std::string clientId = parsedData["clientId"].asString();
    std::string targetId = parsedData["targetId"].asString();
    std::string channel = parsedData["channel"].asString();
    std::string message = parsedData["message"].asString();
    std::string time = parsedData["time"].asString();

    // 验证配对关系
    ValidationResult result;
    if (!this->connectionManager->isPaired(clientId, targetId))
    {
        result.success = false;
        result.code = "402";
        result.message = "配对关系无效";
        return result;
    }

    // 必须指定通道
    if (channel.empty())
    {
        result.success = false;
        result.code = "406";
        result.message = "缺少通道信息";
        return result;
    }

    ClientInfo targetClient;
    bool res = this->connectionManager->getClient(targetId, targetClient);
    if (!res)
    {
        result.success = false;
        result.code = "404";
        result.message = "目标客户端不存在";
        return result;
    }

    // 计算发送参数
    std::map<std::string, std::string> config = readConfig("conf.ini");
    int sendTime = time.empty() ? std::stoi(config["default_punishment_duration"]) : std::stoi(time); // 波形默认持续时间（秒）
    int totalSends = std::stoi(config["default_punishment_time"]) * sendTime;                         // 波形发送次数  hz*s
    int timeSpace = 1000 / std::stoi(config["default_punishment_time"]);                              // 间隔时间（毫秒）。周期T=1/f

    Json::Value plusMessage;
    plusMessage["type"] = "msg";
    plusMessage["clientId"] = clientId;
    plusMessage["targetId"] = targetId;
    plusMessage["message"] = "pulse-" + message;

    // 使用定时器管理器发送消息
    timerManager->sendMessage(clientId, channel, targetClient.hdl, plusMessage, totalSends, timeSpace, hdl);
    std::cout << "[" << clientId << "]" << "波形消息已发送：通道：" << channel << " 次数：" << totalSends << " 时长(s)：" << sendTime << std::endl;

    result.success = true;
    result.code = "200";
    result.message = "波形消息已发送";
    std::map<std::string, std::string> details;
    details["channel"] = channel;
    details["totalSends"] = std::to_string(totalSends);
    details["sendTime"] = std::to_string(sendTime);
    result.details = details;
    return result;
}