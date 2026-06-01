// server.cpp
#include "server.h"

Server::Server()
{
    // 初始化 Asio
    m_server.init_asio();

    // 初始化连接对象
    m_connections = new ConnectionManager(&m_server);
    // 初始化消息路由器
    m_messageRouter = new MessageRouter(m_connections, &m_server);
    // 初始化定时器管理器
    m_timerManager = new TimerManager(&m_server);
    // 初始化心跳定时器
    heartbeatInterval = "";

    m_server.set_open_handler([this](websocketpp::connection_hdl hdl)
                              { on_open(hdl); });
    m_server.set_close_handler([this](websocketpp::connection_hdl hdl)
                               { on_close(hdl); });
    m_server.set_message_handler([this](websocketpp::connection_hdl hdl, server::message_ptr msg)
                                 { on_message(hdl, msg); });
    m_server.set_fail_handler([this](websocketpp::connection_hdl hdl)
                              { on_fail(hdl); });

    // 加载配置文件
    std::map<std::string, std::string> config = readConfig("conf.ini");
    int heartbeat_interval = std::stoi(config["heartbeat_interval"]);

    // 启动定时器(启动一次)
    if (heartbeatInterval.empty())
    {
        heartbeatInterval = JSTimer::generateId();
        JSTimer::setInterval(heartbeatInterval, [this]()
                             { sendHeartbeats(); }, heartbeat_interval);
        std::cout << "心跳定时器已启动，间隔：" << heartbeat_interval << "ms" << std::endl;
    }
}

Server::~Server()
{
    std::cout << "WebSocket 服务器已关闭！" << std::endl;
}

void Server::run(uint16_t port)
{
    // 端口复用
    m_server.set_reuse_addr(true);
    // 监听端口
    m_server.listen(port);
    // 开始接收连接请求
    m_server.start_accept();
    std::cout << "WebSocket 服务器启动，监听端口: " << port << std::endl;
    // 运行服务
    m_server.run();
}

void Server::on_open(websocketpp::connection_hdl hdl)
{
    // 生成 ID
    std::string id = getUUID(this->gen);

    // 获取客户端IP
    // 1. 通过句柄获取连接的智能指针
    server::connection_ptr con = m_server.get_con_from_hdl(hdl);
    // 2. 直接调用 get_remote_endpoint() 获取客户端 IP 和端口
    std::string client_ip = con->get_remote_endpoint();

    // 注册连接信息
    m_connections->registerConn(hdl, id);

    // 发送消息
    Json::Value jsonMsg;
    jsonMsg["type"] = "bind";
    jsonMsg["clientId"] = id;
    jsonMsg["targetId"] = "";
    jsonMsg["message"] = "targetId";
    std::string strMsg = Json::FastWriter().write(jsonMsg);
    try
    {
        m_server.send(hdl, strMsg, websocketpp::frame::opcode::text);
    }
    catch (websocketpp::exception const &e)
    {
        std::cout << "【错误】发送初始绑定信息失败" << e.what() << std::endl;
    }

    // 打印客户端信息
    std::cout << "【新连接】有客户端连接上来，IP：" << client_ip << "\tID：" << id << std::endl;
}

void Server::on_close(websocketpp::connection_hdl hdl)
{
    std::cout << "【连接断开】客户端[" << m_connections->getClientId(hdl) << "]断开连接" << std::endl;

    // 查找配对的另一端并通知
    std::string pairId = m_connections->getPair(m_connections->getClientId(hdl));
    if (!pairId.empty())
    {
        ClientInfo pairClient;
        bool res = m_connections->getClient(pairId, pairClient);
        if (res && m_server.get_con_from_hdl(pairClient.hdl) && m_server.get_con_from_hdl(pairClient.hdl)->get_state() == websocketpp::session::state::open)
        {
            Json::Value jsonMsg;
            jsonMsg["type"] = "error";
            jsonMsg["clientId"] = m_connections->getClientId(hdl);
            jsonMsg["targetId"] = pairId;
            jsonMsg["message"] = "500";
            std::string strMsg = Json::FastWriter().write(jsonMsg);
            try
            {
                m_server.send(pairClient.hdl, strMsg, websocketpp::frame::opcode::text);
            }
            catch (websocketpp::exception const &e)
            {
                std::cout << "【错误】发送错误响应失败" << e.what() << std::endl;
            }
        }
    }

    // 清除该客户端的所有定时器
    m_timerManager->clearClientTimers(m_connections->getClientId(hdl));

    // 断开连接并清理配对关系
    m_connections->disconnect(m_connections->getClientId(hdl));
}

void Server::on_message(websocketpp::connection_hdl hdl, server::message_ptr msg)
{
    // 获取客户端 ID
    std::string clientId = m_connections->getClientId(hdl);
    // 获取消息内容
    std::string payload = msg->get_payload();
    // 打印消息内容
    std::cout << "【收到消息】" << clientId << " : " << payload << std::endl;

    // 验证消息格式
    ValidationResult va = m_messageRouter->validate(payload);
    if (!va.valid)
    {
        // 发送错误响应
        Json::Value jsonMsg;
        jsonMsg["type"] = "msg";
        jsonMsg["clientId"] = "";
        jsonMsg["targetId"] = "";
        jsonMsg["message"] = va.code;
        std::string strMsg = Json::FastWriter().write(jsonMsg);
        try
        {
            m_server.send(hdl, strMsg, websocketpp::frame::opcode::text);
        }
        catch (websocketpp::exception const &e)
        {
            std::cout << "【错误】发送错误响应失败" << e.what() << std::endl;
        }
        return;
    }

    // 验证消息来源(防止消息伪造)
    // v1兼容：发送者的ws必须匹配clientId或targetId其中之一
    if (!m_messageRouter->validateSource(va.data["clientId"].asString(), hdl) && !m_messageRouter->validateSource(va.data["targetId"].asString(), hdl))
    {
        // 发送错误响应
        Json::Value jsonMsg;
        jsonMsg["type"] = "msg";
        jsonMsg["clientId"] = "";
        jsonMsg["targetId"] = "";
        jsonMsg["message"] = "404";
        std::string strMsg = Json::FastWriter().write(jsonMsg);
        try
        {
            m_server.send(hdl, strMsg, websocketpp::frame::opcode::text);
        }
        catch (websocketpp::exception const &e)
        {
            std::cout << "【错误】发送错误响应失败" << e.what() << std::endl;
        }
        return;
    }

    // 根据消息类型路由处理
    std::string msgType = va.data["type"].asString();
    if (msgType == "bind")
    {
        // 处理绑定请求
        std::cout << "============================================" << std::endl;
        std::cout << "==                 处理绑定请求            ==" << std::endl;
        std::cout << "============================================" << std::endl;
        handleBind(va.data, hdl);
    }
    else if (msgType == "1" || msgType == "2" || msgType == "3")
    {
        // 处理强度调节
        std::cout << "============================================" << std::endl;
        std::cout << "==                 处理强度调节            ==" << std::endl;
        std::cout << "============================================" << std::endl;
        handleStrengthAdjust(va.data, hdl);
    }
    else if (msgType == "4")
    {
        // 转发网页端到APP  message内容直接作为 APP 指令转发
        std::cout << "============================================" << std::endl;
        std::cout << "==               转发网页端到APP           ==" << std::endl;
        std::cout << "============================================" << std::endl;
        handleCustomStrength(va.data, hdl);
    }
    else if (msgType == "clientMsg")
    {
        // 处理客户端消息（波形数据等）
        std::cout << "============================================" << std::endl;
        std::cout << "==               处理客户端消息            ==" << std::endl;
        std::cout << "============================================" << std::endl;
        handleClientMessage(va.data, hdl);
    }
    else if (msgType == "heartbeat")
    {
        // 处理心跳
        handleHeartbeat(va.data, hdl);
    }
    else
    {
        // 处理转发消息 (app->web)
        std::cout << "============================================" << std::endl;
        std::cout << "==               处理转发消息             ==" << std::endl;
        std::cout << "============================================" << std::endl;
        forwardMessage(va.data, hdl);
    }
}

void Server::on_fail(websocketpp::connection_hdl hdl)
{
    std::cout << "【连接失败】WebSocket异常" << std::endl;
}

void Server::handleBind(Json::Value data, websocketpp::connection_hdl hdl)
{
    std::string strData = Json::FastWriter().write(data);
    auto result = m_messageRouter->handleBind(strData, hdl);
    std::cout << "【绑定请求】[" << m_connections->getClientId(hdl) << "] : code=" << result.code << std::endl;
}

void Server::handleStrengthAdjust(Json::Value data, websocketpp::connection_hdl hdl)
{
    std::string strData = Json::FastWriter().write(data);
    auto result = m_messageRouter->handleStrengthAdjust(strData, hdl);

    if (!result.success)
    {
        // 发送错误响应
        Json::Value jsonMsg;
        jsonMsg["type"] = "error";
        jsonMsg["clientId"] = data["clientId"].asString();
        jsonMsg["targetId"] = data["targetId"].asString();
        jsonMsg["message"] = result.code;
        std::string strMsg = Json::FastWriter().write(jsonMsg);
        try
        {
            m_server.send(hdl, strMsg, websocketpp::frame::opcode::text);
        }
        catch (websocketpp::exception const &e)
        {
            std::cout << "【错误】发送错误响应失败" << e.what() << std::endl;
        }
    }
}

void Server::handleCustomStrength(Json::Value data, websocketpp::connection_hdl hdl)
{
    std::string strData = Json::FastWriter().write(data);
    auto result = m_messageRouter->handleCustomStrength(strData, hdl);
    if (!result.success)
    {
        // 发送错误响应
        Json::Value jsonMsg;
        jsonMsg["type"] = "error";
        jsonMsg["clientId"] = data["clientId"].asString();
        jsonMsg["targetId"] = data["targetId"].asString();
        jsonMsg["message"] = result.code;
        std::string strMsg = Json::FastWriter().write(jsonMsg);
        try
        {
            m_server.send(hdl, strMsg, websocketpp::frame::opcode::text);
        }
        catch (websocketpp::exception const &e)
        {
            std::cout << "【错误】发送错误响应失败" << e.what() << std::endl;
        }
    }
}

void Server::handleClientMessage(Json::Value data, websocketpp::connection_hdl hdl)
{
    std::string strData = Json::FastWriter().write(data);
    auto result = m_messageRouter->handleClientMessage(strData, hdl, m_timerManager);
    if (!result.success)
    {
        std::cout << "【错误】" << "handleClientMessage" << " : " << result.code << std::endl;
        // 发送错误响应
        Json::Value jsonMsg;
        jsonMsg["type"] = (data["type"].asString() == "clientMsg" ? "error" : "bind");
        jsonMsg["clientId"] = data["clientId"].asString();
        jsonMsg["targetId"] = data["targetId"].asString();
        jsonMsg["message"] = result.code;
        std::string strMsg = Json::FastWriter().write(jsonMsg);
        try
        {
            m_server.send(hdl, strMsg, websocketpp::frame::opcode::text);
        }
        catch (websocketpp::exception const &e)
        {
            std::cout << "【错误】发送错误响应失败" << e.what() << std::endl;
        }
    }
}

void Server::handleHeartbeat(Json::Value data, websocketpp::connection_hdl hdl)
{
    // 这里是我自己添加的一个函数，dglab源码中没有接收心跳的操作，而是发出心跳包之后直接就更新了心跳时间，因为不敢对源码进行修改，所以先不做处理，打印一下。
    std::cout << "【心跳】收到心跳消息" << std::endl;
}

void Server::forwardMessage(Json::Value data, websocketpp::connection_hdl hdl)
{
    std::string strData = Json::FastWriter().write(data);
    auto result = m_messageRouter->forwardMessage(strData, hdl);
    if (!result.success)
    {
        // 发送错误响应
        Json::Value jsonMsg;
        jsonMsg["type"] = "msg";
        jsonMsg["clientId"] = data["clientId"].asString();
        jsonMsg["targetId"] = data["targetId"].asString();
        jsonMsg["message"] = result.code;
        std::string strMsg = Json::FastWriter().write(jsonMsg);
        try
        {
            m_server.send(hdl, strMsg, websocketpp::frame::opcode::text);
        }
        catch (websocketpp::exception const &e)
        {
            std::cout << "【错误】发送错误响应失败" << e.what() << std::endl;
        }
    }
}

void Server::sendHeartbeats()
{
    Json::Value heartbeatMsg;
    heartbeatMsg["type"] = "heartbeat";
    heartbeatMsg["clientId"] = "";
    heartbeatMsg["targetId"] = "";
    heartbeatMsg["message"] = "200";

    std::map<std::string, std::string> stats = m_connections->getStats();
    std::cout << "发送心跳，当前连接数：" << stats["totalConnections"] << ", 配对数：" << stats["pairedConnections"] << std::endl;

    m_connections->forEachClient([&](const std::string &clientId, const ClientInfo &client)
                                 {
        if (m_server.get_con_from_hdl(client.hdl) && m_server.get_con_from_hdl(client.hdl)->get_state() == websocketpp::session::state::open)
        {
            try
            {
                heartbeatMsg["clientId"] = clientId;
                heartbeatMsg["targetId"] = (m_connections->getPair(clientId) == "" ? "" : m_connections->getPair(clientId));
                std::string strMsg = Json::FastWriter().write(heartbeatMsg);
                m_server.send(client.hdl, strMsg, websocketpp::frame::opcode::text);

                // 更新心跳时间
                m_connections->updateHeartbeat(clientId);
            }
            catch(const websocketpp::exception& e)
            {
                std::cout << "【发送心跳失败】" << clientId << " : " << e.what() << std::endl;
            }
            catch(const std::exception& e)
            {
                std::cout << "【发送心跳失败】" << clientId << " : " << e.what() << std::endl;
            }
            
        } });
}

void Server::gracefulShutdown(int signal)
{
    std::cout << "收到信号：" << signal << ", 准备关闭 WebSocket 服务器..." << std::endl;

    // 清理心跳定时器
    if (!heartbeatInterval.empty())
    {
        JSTimer::clearInterval(heartbeatInterval);
        heartbeatInterval = "";
    }
    std::cout << "已关闭心跳定时器" << std::endl;

    // 关闭所有客户端连接
    if (m_connections != nullptr)
    {
        delete m_connections;
        m_connections = nullptr;
    }
    std::cout << "释放连接管理器" << std::endl;

    if (m_messageRouter != nullptr)
    {
        delete m_messageRouter;
        m_messageRouter = nullptr;
    }
    std::cout << "释放消息路由器" << std::endl;

    if (m_timerManager != nullptr)
    {
        delete m_timerManager;
        m_timerManager = nullptr;
    }
    std::cout << "释放定时器管理器" << std::endl;

    // 停止服务
    m_server.stop();

    std::cout << "已关闭所有客户端连接" << std::endl;
}