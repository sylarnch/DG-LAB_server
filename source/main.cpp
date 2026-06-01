// main.cpp
#include "server.h"
#include <csignal>

Server *sigServer = nullptr;

void signalHandler(int signal)
{
    if (signal == SIGINT || signal == SIGTERM)
    {
        if (sigServer != nullptr)
        {
            sigServer->gracefulShutdown(signal);
        }
    }
}

int main()
{
    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);
    signal(SIGBREAK, signalHandler);

    // 获取端口号
    std::map<std::string, std::string> config = readConfig("conf.ini");
    int port = 0;
    try
    {
        port = std::stoi(config["port"]);
    }
    catch (const std::exception &e)
    {
        std::cout << "配置文件中端口号格式错误！" << std::endl;
        std::cout << "读取到的端口号：" << config["port"] << std::endl;
        return 1;
    }

    // 创建 WebSocket 服务器实例
    Server server;
    sigServer = &server;

    // 启动服务
    server.run(port);

    return 0;
}