// timer.cpp
#include "timer.h"
#include "JSTimer.hpp"
#include <websocketpp/error.hpp>

TimerManager::TimerManager(WsServer *server)
{
    this->server = server;
}
TimerManager::~TimerManager()
{
    // 清除所有定时器
    cleanupAll();
}

void TimerManager::sendMessage(std::string clientId, std::string channel, websocketpp::connection_hdl targetHdl, Json::Value message, int totalSends, int timeSpace, websocketpp::connection_hdl sourceHdl)
{
    std::string timerKey = clientId + "-" + channel;

    // 检查是否已有该通道的定时器在运行
    if (this->timers.find(timerKey) != this->timers.end())
    {
        // 有正在运行的定时器，需要先清除（覆盖旧消息）
        std::cout << "[" << timerKey << "] 清除现有定时器，准备发送新消息" << std::endl;
        TimerInfo oldTask = this->timers.at(timerKey);
        this->clearTimer(clientId, channel, oldTask.sourcehdl);

        // 发送清除 APP 队列指令
        Json::Reader reader;
        std::string me = (channel == "A" ? "1" : "2");
        Json::Value clearMessage;
        clearMessage["type"] = "msg";
        clearMessage["clientId"] = clientId;
        clearMessage["targetId"] = oldTask.message["targetId"];
        clearMessage["message"] = "clear-" + me;
        std::string clearStr = Json::FastWriter().write(clearMessage);
        try
        {
            this->server->send(targetHdl, clearStr, websocketpp::frame::opcode::text);
        }
        catch (const websocketpp::exception &e)
        {
            std::cout << "发送清除指令失败：" << e.what() << std::endl;
        }
        catch (const std::exception &e)
        {
            std::cout << "发送清除指令失败：" << e.what() << std::endl;
        }

        // 延迟 150ms 再发送新消息，避免队列指令晚于波形数据执行
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
        this->_startSending(clientId, channel, targetHdl, message, totalSends, timeSpace, sourceHdl);

        // 通知客户端正在覆盖
        if (this->server->get_con_from_hdl(sourceHdl) && this->server->get_con_from_hdl(sourceHdl)->get_state() == websocketpp::session::state::open)
        {
            Json::Value notifyMessage;
            notifyMessage["type"] = "notify";
            notifyMessage["clientId"] = clientId;
            notifyMessage["targetId"] = "";
            notifyMessage["message"] = "当前通道" + channel + "有正在发送的消息，覆盖之前的消息";
            std::string notifyStr = Json::FastWriter().write(notifyMessage);
            try
            {
                this->server->send(sourceHdl, notifyStr, websocketpp::frame::opcode::text);
            }
            catch (const websocketpp::exception &e)
            {
                std::cout << "发送覆盖通知失败：" << e.what() << std::endl;
            }
            catch (const std::exception &e)
            {
                std::cout << "发送覆盖通知失败：" << e.what() << std::endl;
            }
        }
        return;
    }
    // 不存在未发完的消息，直接开始发送
    this->_startSending(clientId, channel, targetHdl, message, totalSends, timeSpace, sourceHdl);
}

void TimerManager::_startSending(std::string clientId, std::string channel, websocketpp::connection_hdl targetHdl, Json::Value message, int totalSends, int timeSpace, websocketpp::connection_hdl sourceHdl)
{
    std::string timerKey = clientId + "-" + channel;

    // 创建定时器任务
    TimerInfo timerTask;
    timerTask.clientId = clientId;
    timerTask.channel = channel;
    timerTask.targethdl = targetHdl;
    timerTask.sourcehdl = sourceHdl;
    timerTask.message = message;
    timerTask.remaining = totalSends;
    timerTask.timeSpace = timeSpace;
    timerTask.timerId = "";
    timerTask.startedAt = std::chrono::system_clock::now();

    // 立即发送第一条消息
    this->_safeSend(targetHdl, message);
    timerTask.remaining--;

    std::cout << "[" << timerKey << "] 消息发送中，剩余次数：" << timerTask.remaining << std::endl;

    // 如果还有剩余次数，启动定时器
    if (timerTask.remaining > 0)
    {
        std::string timerId = JSTimer::generateId();
        JSTimer::setInterval(timerId, [this, targetHdl, timerKey, timerId, message, timerTask, sourceHdl, clientId]() mutable
                             {
            // 1. 检查目标连接是否还存在
            if (server->get_con_from_hdl(targetHdl) && server->get_con_from_hdl(targetHdl)->get_state() != websocketpp::session::state::open)
            {
                std::cout << "[" << timerKey << "] 目标连接已断开，停止发送" << std::endl;
                JSTimer::clearInterval(timerId);
                this->timers.erase(timerKey);
                return;
            }

            this->_safeSend(targetHdl, message);
            timerTask.remaining--;

            if (timerTask.remaining <= 0)
            {
                std::cout << "[" << timerKey << "] 消息发送完毕" << std::endl;
                JSTimer::clearInterval(timerId);
                this->timers.erase(timerKey);

                // 通知客户端发送完毕
                if (this->server->get_con_from_hdl(sourceHdl) && this->server->get_con_from_hdl(sourceHdl)->get_state() == websocketpp::session::state::open)
                {
                    try
                    {
                        Json::Value notify_json;
                        notify_json["type"] = "notify";
                        notify_json["clientId"] = clientId;
                        notify_json["targetId"] = message["targetId"];
                        notify_json["message"] = "发送完毕";
                        std::string notify_str = Json::FastWriter().write(notify_json);
                        server->send(sourceHdl, notify_str, websocketpp::frame::opcode::text);
                    }
                    catch (const websocketpp::exception &e)
                    {
                        std::cerr << "发送完成通知失败：" << e.what() << std::endl;
                    }
                    catch (const std::exception &e)
                    {
                        std::cerr << "发送完成通知失败：" << e.what() << std::endl;
                    }
                }

            } }, timeSpace);

        timerTask.timerId = timerId;
        this->timers.insert(std::make_pair(timerKey, timerTask));
    }
    else
    {
        std::cout << "[" << timerKey << "] 消息已发送完成（仅 1 条）" << std::endl;
        // 通知客户端发送完毕
        if (this->server->get_con_from_hdl(sourceHdl) && this->server->get_con_from_hdl(sourceHdl)->get_state() == websocketpp::session::state::open)
        {
            try
            {
                Json::Value notify_json;
                notify_json["type"] = "notify";
                notify_json["clientId"] = clientId;
                notify_json["targetId"] = timerTask.message["targetId"];
                notify_json["message"] = "发送完毕";
                std::string notify_str = Json::FastWriter().write(notify_json);
                server->send(sourceHdl, notify_str, websocketpp::frame::opcode::text);
            }
            catch (const websocketpp::exception &e)
            {
                std::cerr << "发送完成通知失败：" << e.what() << std::endl;
            }
            catch (const std::exception &e)
            {
                std::cerr << "发送完成通知失败：" << e.what() << std::endl;
            }
        }
    }
}

void TimerManager::clearTimer(std::string clientId, std::string channel, websocketpp::connection_hdl sourceHdl)
{
    std::string timerKey = clientId + "-" + channel;
    if (timers.find(timerKey) == timers.end())
    {
        std::cout << "[" << timerKey << "] 定时器不存在" << std::endl;
        return;
    }
    TimerInfo timerTask = this->timers.at(timerKey);

    // 先发送清除指令
    Json::Value clearMessage;
    clearMessage["type"] = "msg";
    clearMessage["clientId"] = clientId;
    clearMessage["targetId"] = timerTask.message["targetId"];
    std::string me = channel == "A" ? "1" : "2";
    clearMessage["message"] = "clear-" + me;
    this->_safeSend(timerTask.targethdl, clearMessage);

    // 清除定时器
    if (timerTask.timerId != "")
    {
        JSTimer::clearInterval(timerTask.timerId);
    }
    this->timers.erase(timerKey);
    std::cout << "[" << timerKey << "] 定时器已清除" << std::endl;
}

void TimerManager::clearClientTimers(std::string clientId)
{
    std::vector<std::string> keysToDelete;
    for (const auto &[key, task] : this->timers)
    {
        // 使用 find 查找前缀，如果返回的位置是 0，说明是以该前缀开头的 (对应 JS 的 startsWith)
        if (key.find(clientId + "-") == 0)
        {
            keysToDelete.push_back(key);
        }
    }

    for (const auto &key : keysToDelete)
    {
        auto it = this->timers.find(key);

        if (it != this->timers.end())
        {
            auto &task = it->second;
            JSTimer::clearInterval(task.timerId);
        }
        this->timers.erase(key);
    }
    std::cout << "[" << clientId << "] 清除了 " << keysToDelete.size() << " 个定时器" << std::endl;
}

void TimerManager::_safeSend(websocketpp::connection_hdl hdl, Json::Value message)
{
    if (server->get_con_from_hdl(hdl) && server->get_con_from_hdl(hdl)->get_state() == websocketpp::session::state::open)
    {
        try
        {
            std::string message_str = Json::FastWriter().write(message);
            server->send(hdl, message_str, websocketpp::frame::opcode::text);
        }
        catch (const std::exception &e)
        {
            std::cout << "发送消息失败：" << e.what() << std::endl;
        }
        catch (const websocketpp::exception &e)
        {
            std::cout << "发送消息失败：" << e.what() << std::endl;
        }
    }
}

Json::Value TimerManager::getStats()
{
    Json::Value stats;
    stats["activeTimers"] = this->timers.size();
    Json::Value timers_array;
    for (const auto &[key, task] : this->timers)
    {
        Json::Value timer_json;
        timer_json["key"] = key;
        timer_json["clientId"] = task.clientId;
        timer_json["channel"] = task.channel;
        timer_json["remaining"] = task.remaining;
        timer_json["startedAt"] = task.startedAt.time_since_epoch().count();
        timers_array.append(timer_json);
    }
    stats["timers"] = timers_array;
    return stats;
}

void TimerManager::cleanupAll()
{
    for (const auto &[key, task] : this->timers)
    {
        if (task.timerId != "")
        {
            JSTimer::clearInterval(task.timerId);
        }
    }
    this->timers.clear();
    std::cout << "所有定时器已清理" << std::endl;
}