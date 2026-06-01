// JSTimer.hpp
#ifndef JSTIMER_HPP
#define JSTIMER_HPP

#include <atomic>
#include <string>
#include <mutex>
#include <unordered_map>
#include <functional>

class JSTimer
{
private:
    // 线程安全的全局计数器，用于生成唯一的数字ID
    inline static std::atomic<int> global_counter{0};
    // 保护定时器映射表（timer_map）的互斥锁
    inline static std::mutex map_mutex;
    // 存储所有活跃定时器的映射表：ID -> 定时器对象指针
    inline static std::unordered_map<std::string, JSTimer *> timer_map;

    std::atomic<bool> active{false}; // 控制当前定时器是否活跃的原子标志
    std::thread timer_thread;        // 承载定时任务的线程
    std::string my_id;               // 记录当前定时器自己的ID

public:
    ~JSTimer()
    {
        stop();
    }

    // id生成
    static std::string generateId()
    {
        // 1. 生成唯一的数字字符串 ID (例如 "1", "2", "3"...)
        int current_id_num = ++global_counter;
        std::string id_str = std::to_string(current_id_num);
        return id_str;
    }

    // 复现 setInterval：周期性执行任务，返回 string 类型的 timerID
    static void setInterval(std::string timer_id, std::function<void()> callback, int interval_ms)
    {
        // 2. 创建新的定时器对象
        JSTimer *new_timer = new JSTimer();
        new_timer->my_id = timer_id;

        // 3. 启动后台线程并加入全局映射表
        {
            std::lock_guard<std::mutex> lock(map_mutex);
            timer_map[timer_id] = new_timer;
        }

        new_timer->active = true;
        new_timer->timer_thread = std::thread([new_timer, callback, interval_ms]() mutable
                                              {
            while (new_timer->active.load()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
                if (new_timer->active.load()) {
                    callback();
                }
            }
            // 任务结束后，尝试从全局映射表中删除自己（如果还存在的话）
            {
                std::lock_guard<std::mutex> lock(map_mutex);
                auto it = timer_map.find(new_timer->my_id);
                if (it != timer_map.end() && it->second == new_timer) {
                    timer_map.erase(it);
                    if (new_timer!=nullptr)
                    {
                        delete new_timer;
                        new_timer = nullptr;
                    }
                }
            } });
        new_timer->timer_thread.detach();
    }

    // 复现 clearInterval：根据 string 类型的ID清除/停止定时器
    static void clearInterval(const std::string timer_id)
    {
        // std::lock_guard<std::mutex> lock(map_mutex);
        auto it = timer_map.find(timer_id);
        if (it != timer_map.end())
        {
            it->second->stop(); // 找到对应的定时器对象，调用其内部的停止逻辑
        }
    }

private:
    // 内部停止逻辑
    void stop()
    {
        if (active.load())
        {
            active = false;
            // if (timer_thread.joinable())
            // {
            //     timer_thread.join();
            // }
        }
    }
};

// // 静态成员变量的初始化
// std::atomic<int> JSTimer::global_counter(0);
// std::mutex JSTimer::map_mutex;
// std::unordered_map<std::string, JSTimer *> JSTimer::timer_map;

#endif // JSTIMER_HPP
