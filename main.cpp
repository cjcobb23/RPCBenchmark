
#include <boost/asio.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <condition_variable>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <json/json.h>
#include <optional>
#include <string>
#include <thread>

namespace beast = boost::beast;          // from <boost/beast.hpp>
namespace http = beast::http;            // from <boost/beast/http.hpp>
namespace websocket = beast::websocket;  // from <boost/beast/websocket.hpp>
namespace net = boost::asio;             // from <boost/asio.hpp>
using tcp = boost::asio::ip::tcp;        // from <boost/asio/ip/tcp.hpp>

// Sends a WebSocket message and prints the response
int
main(int argc, char** argv)
{
    try
    {
        // Check command line arguments.
        if (argc < 7)
        {
            std::cerr << "Usage: websocket-client-sync <host> <port> \n"
                      << "Example:\n"
                      << "    websocket-client-sync echo.websocket.org 80 "
                         "\"Hello, world!\"\n";
            return EXIT_FAILURE;
        }
        std::string host = argv[1];
        auto const port = argv[2];
        size_t numRequests = std::atol(argv[3]);
        size_t numReaders = std::atol(argv[4]);
        bool readFromFile = false;
        if (argc > 6)
        {
            std::ifstream f(argv[5]);
            readFromFile = f.good();
            f.close();
        }

        bool tx = (argv[6][0] == 't');
        bool account_info = (argv[6][0] == 'a');
        bool ledgers = (argv[6][0] == 'l');

        bool verify = argc > 7;

        // The io_context is required for all I/O
        net::io_context ioc;

        // These objects perform our I/O
        tcp::resolver resolver{ioc};
        websocket::stream<tcp::socket> ws{ioc};

        // Look up the domain name
        auto const results = resolver.resolve(host, port);

        // Make the connection on the IP address we get from a lookup
        auto ep = net::connect(ws.next_layer(), results);

        // Update the host_ string. This will provide the value of the
        // Host HTTP header during the WebSocket handshake.
        // See https://tools.ietf.org/html/rfc7230#section-5.4
        std::string updatedHost = host + ':' + std::to_string(ep.port());

        // Set a decorator to change the User-Agent of the handshake
        ws.set_option(
            websocket::stream_base::decorator([](websocket::request_type& req) {
                req.set(
                    http::field::user_agent,
                    std::string(BOOST_BEAST_VERSION_STRING) +
                        " websocket-client-coro");
            }));

        // Perform the websocket handshake
        ws.handshake(updatedHost, "/");

        Json::Value request;
        request["command"] = "ledger";
        request["transactions"] = "true";
        request["ledger_index"] = "validated";
        std::vector<Json::Value> hashes;

        std::vector<Json::Value> accounts;
        if (readFromFile and not ledgers)
        {
            std::ifstream f(argv[5]);
            while (f.good())
            {
                std::string line;
                std::getline(f, line);
                if (line.empty())
                    continue;
                line = line.substr(1, line.size() - 2);
                if (tx)
                    hashes.emplace_back(line);
                else if(account_info)
                    accounts.emplace_back(line);
            }
            f.close();
        }
        else
        {
            while (tx and hashes.size() < numRequests)
            {
                // request["ledger_index"] = "validated";

                Json::StreamWriterBuilder writer;

                // Send the message
                ws.write(net::buffer(Json::writeString(writer, request)));

                // This buffer will hold the incoming message
                beast::flat_buffer buffer;

                // Read a message into our buffer
                ws.read(buffer);

                Json::CharReaderBuilder builder;
                Json::CharReader* reader{builder.newCharReader()};

                Json::Value response;
                const char* data =
                    static_cast<const char*>(buffer.data().data());
                const char* dataEnd = data + buffer.data().size();
                reader->parse(data, dataEnd, &response, nullptr);

                // std::cout << "got message : " << response << std::endl;

                request["ledger_index"] =
                    std::stol(response["result"]["ledger"]["ledger_index"]
                                  .asString()) -
                    1;

                for (auto& jv : response["result"]["ledger"]["transactions"])
                {
                    hashes.push_back(jv);
                }
            }

            request["command"] = "ledger_data";
            while (account_info and accounts.size() < numRequests)
            {
                Json::StreamWriterBuilder writer;
                ws.write(net::buffer(Json::writeString(writer, request)));

                beast::flat_buffer buffer;
                ws.read(buffer);

                Json::CharReaderBuilder builder;
                Json::CharReader* reader{builder.newCharReader()};

                Json::Value response;
                const char* data =
                    static_cast<const char*>(buffer.data().data());
                const char* dataEnd = data + buffer.data().size();
                reader->parse(data, dataEnd, &response, nullptr);

                for (auto& jv : response["result"]["state"])
                {
                    if (jv["LedgerEntryType"] == "AccountRoot")
                        accounts.push_back(jv["Account"]);
                }
//                std::cout << response << std::endl;
                if (response["result"].isMember("marker"))
                    request["marker"] = response["result"]["marker"];
                else
                    break;
            }

        }

        std::cout << "gathered request data" << std::endl;
        std::cout << "accounts " << accounts.size() << std::endl;
        std::cout << "tx " << hashes.size() << std::endl;

        std::ofstream myFile;
        if (!readFromFile and argc > 5)
            myFile.open(argv[5]);
        std::vector<Json::Value> requests;
        if (tx)
        {
            for (auto& tx : hashes)
            {
                Json::Value req;
                req["command"] = "tx";
                req["transaction"] = tx;
                requests.push_back(req);
                if (!readFromFile and argc > 5)
                    myFile << tx << '\n';
            }
        }
        else if(account_info)
        {
            for (auto& a : accounts)
            {
                Json::Value req;
                req["command"] = "account_info";
                req["account"] = a;
                requests.push_back(req);
                if (!readFromFile and argc > 5)
                    myFile << a << '\n';
            }
        }
        else if(ledgers)
        {
            for(size_t i = 0; i < numRequests; ++i)
            {
                Json::Value req;
                req["command"] = "ledger";
                req["transactions"] = "true";
                req["expand"] = "true";
                requests.push_back(req);
            }
        }

        std::cout << "requests = " << requests.size() << std::endl;
        if (requests.size() > numRequests)
            requests.erase(requests.begin() + numRequests, requests.end());


        if (myFile.is_open())
            myFile.close();

        struct Reader
        {
            size_t offset;
            size_t incr;
            std::vector<Json::Value>& requests;
            beast::flat_buffer readBuffer;
            net::io_context& ioc;
            boost::beast::websocket::stream<boost::beast::tcp_stream> ws;
            std::atomic_uint32_t& counter;
            std::mutex mtx;
            std::condition_variable cv;
            std::atomic_bool connected{false};
            tcp::resolver resolver;
            bool verify;
            Reader(
                size_t offset,
                size_t incr,
                std::string host,
                std::string port,
                std::vector<Json::Value>& requests,
                net::io_context& ioc,
                std::atomic_uint32_t& counter,
                bool verify = false)
                : offset(offset)
                , incr(incr)
                , requests(requests)
                , ioc(ioc)
                , ws(ioc)
                , counter(counter)
                , resolver(ioc)
                , verify(verify)
            {
                // These objects perform our I/O


                resolver.async_resolve(
                    host, port, [this, host](auto ec, auto results) {
                        boost::beast::get_lowest_layer(ws).expires_after(
                            std::chrono::seconds(30));
                        boost::beast::get_lowest_layer(ws).async_connect(
                            results, [this, host](auto ec, auto ep) {
                                boost::beast::get_lowest_layer(ws)
                                    .expires_never();
                                // Set suggested timeout settings for the
                                // websocket
                                ws.set_option(
                                    boost::beast::websocket::stream_base::
                                        timeout::suggested(
                                            boost::beast::role_type::client));

                                // Set a decorator to change the User-Agent of
                                // the handshake
                                ws.set_option(
                                    boost::beast::websocket::stream_base::
                                        decorator([](boost::beast::websocket::
                                                         request_type& req) {
                                            req.set(
                                                boost::beast::http::field::
                                                    user_agent,
                                                std::string(
                                                    BOOST_BEAST_VERSION_STRING) +
                                                    " websocket-client-async");
                                        }));

                                // Update the host_ string. This will provide
                                // the value of the Host HTTP header during the
                                // WebSocket handshake. See
                                // https://tools.ietf.org/html/rfc7230#section-5.4

                                std::string hostUpd =
                                    host + ':' + std::to_string(ep.port());
                                // Perform the websocket handshake
                                ws.async_handshake(host, "/", [this](auto ec) {
                                    connected = true;
                                    cv.notify_all();
                                });
                            });
                    });
            }

            void
            start()
            {
                std::unique_lock<std::mutex> lck(mtx);
                cv.wait(lck, [this]() { return connected == true; });

                if (offset > requests.size())
                    return;
                Json::StreamWriterBuilder writer;
                ws.async_write(
                    net::buffer(Json::writeString(writer, requests[offset])),
                    [this](auto ec, auto size) { onWrite(ec, size); });
            }
            void
            onWrite(boost::beast::error_code ec, size_t size)
            {
                ws.async_read(readBuffer, [this](auto ec, auto size) {
                    onRead(ec, size);
                });
            }

            void
            onRead(boost::beast::error_code ec, size_t size)
            {
                if (verify)
                {
                    std::cout << ec << std::endl;

                    Json::CharReaderBuilder builder;
                    Json::CharReader* reader{builder.newCharReader()};

                    Json::Value response;
                    const char* data =
                        static_cast<const char*>(readBuffer.data().data());
                    const char* dataEnd = data + readBuffer.data().size();
                    std::string raw{data, dataEnd};
                    std::string errs;
                    bool res = reader->parse(data, dataEnd, &response, &errs);
                    std::cout << errs << std::endl;
                    std::cout << raw << std::endl;
                    std::cout << response << std::endl;
                    assert(res);
                    assert(response["status"] == "success");
                }
                beast::flat_buffer buf;
                swap(buf, readBuffer);
                uint32_t count = ++counter;
                offset += incr;
                if (offset >= requests.size())
                    return;
                if (count % 1000 == 0)
                    std::cout << count << std::endl;
                Json::StreamWriterBuilder writer;
                ws.async_write(
                    net::buffer(Json::writeString(writer, requests[offset])),
                    [this](auto ec, auto size) { onWrite(ec, size); });
            }
        };

        std::vector<std::shared_ptr<Reader>> readers;
        std::atomic_uint32_t counter{0};

        for (size_t i = 0; i < numReaders; ++i)
        {
            readers.push_back(std::make_shared<Reader>(
                i, numReaders, host, port, requests, ioc, counter, verify));
        }
        std::optional<boost::asio::io_service::work> work;
        work.emplace(ioc);
        std::thread iothread{[&ioc]() { ioc.run(); }};
        for (auto& reader : readers)
        {
            reader->start();
        }
        work.reset();

        auto start = std::chrono::system_clock::now();
        iothread.join();
        auto end = std::chrono::system_clock::now();
        auto diff = (end - start).count() / 1000000000.0;
        std::cout << "finished requests in " << diff << " seconds."
                  << " Requests per second = " << (requests.size() / diff)
                  << std::endl;
        assert(counter == requests.size());

        // Close the WebSocket connection
        ws.close(websocket::close_code::normal);

        // If we get here then the connection is closed gracefully

        // The make_printable() function helps print a ConstBufferSequence
        // std::cout << beast::make_printable(buffer.data()) << std::endl;
    }
    catch (std::exception const& e)
    {
        std::cerr << "Error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}

