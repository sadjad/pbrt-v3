/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#ifndef PBRT_EXECUTION_LOOP_H
#define PBRT_EXECUTION_LOOP_H

#include <list>
#include <vector>
#include <functional>
#include <unordered_map>
#include <type_traits>

#include "connection.h"
#include "net/http_response.h"
#include "net/http_response_parser.h"
#include "net/socket.h"
#include "net/nb_secure_socket.h"
#include "util/signalfd.h"
#include "util/child_process.h"
#include "util/poller.h"
#include "storage/backend_s3.h"

class ExecutionLoop
{
public:
  typedef std::function<void( const uint64_t /* id */,
                              const std::string & /* tag */,
                              const int /* exit_status */ )> LocalCallbackFunc;
  typedef std::function<void( const uint64_t id /* id */,
                              const std::string & /* tag */,
                              const HTTPResponse & )> HTTPResponseCallbackFunc;
  typedef std::function<void( const uint64_t /* id */,
                              const std::string & /* tag */ )> FailureCallbackFunc;
  typedef std::function<void( const uint64_t /* id */,
                              const std::string & /* tag */)> DownloadCallback;

private:
  uint64_t current_id_{ 0 };

  SignalMask signals_;
  SignalFD signal_fd_;

  Poller poller_ {};
  std::list<std::tuple<uint64_t, bool, LocalCallbackFunc, ChildProcess>> child_processes_ {};
  std::list<std::shared_ptr<TCPConnection>> connections_ {};
  std::list<std::shared_ptr<SSLConnection>> ssl_connections_ {};
  std::list<std::shared_ptr<UDPConnection>> udp_connections_ {};

  SSLContext ssl_context_ {};

  Poller::Action::Result handle_signal( const signalfd_siginfo & );

  template<typename SocketType>
  typename std::list<std::shared_ptr<Connection<SocketType>>>::iterator create_connection( SocketType && socket );

  std::list<std::shared_ptr<UDPConnection>>::iterator create_connection( UDPSocket && socket );

  template<typename ConnectionType>
  void remove_connection( const typename std::list<std::shared_ptr<ConnectionType>>::iterator & it );

public:
  ExecutionLoop();

  uint64_t add_child_process( const std::string & tag,
                              LocalCallbackFunc callback,
                              std::function<int()> && child_procedure,
                              const bool throw_if_failed = true );

  template<class SocketType>
  std::shared_ptr<Connection<SocketType>>
  add_connection( SocketType && socket,
                  const std::function<bool(std::shared_ptr<Connection<SocketType>>,
                                           std::string &&)> & data_callback,
                  const std::function<void()> & error_callback = [](){},
                  const std::function<void()> & close_callback = [](){} );

  std::shared_ptr<UDPConnection>
  add_connection( UDPSocket && socket,
                  const std::function<bool(std::shared_ptr<UDPConnection>,
                                           std::string &&)> & data_callback,
                  const std::function<void()> & error_callback = [](){},
                  const std::function<void()> & close_callback = [](){} );


  template<class ConnectionType>
  std::shared_ptr<ConnectionType>
  make_connection( const Address & address,
                   const std::function<bool(std::shared_ptr<ConnectionType>,
                                            std::string &&)> & data_callback,
                   const std::function<void()> & error_callback = [](){},
                   const std::function<void()> & close_callback = [](){} );

  std::shared_ptr<UDPConnection>
  make_udp_connection( const std::function<bool(std::shared_ptr<UDPConnection>,
                                                Address &&,
                                                std::string &&)> & data_callback,
                       const std::function<void()> & error_callback = [](){},
                       const std::function<void()> & close_callback = [](){} );

  template<class ConnectionType>
  uint64_t make_http_request( const std::string & tag,
                              const Address & address,
                              const HTTPRequest & request,
                              HTTPResponseCallbackFunc response_callback,
                              FailureCallbackFunc failure_callback );

  uint64_t s3_download( const std::string & tag,
                        const S3StorageBackend & storage_backend,
                        const std::string & object,
                        const std::string & download_path,
                        DownloadCallback download_callback,
                        FailureCallbackFunc failure_callback );

  uint64_t make_listener( const Address & address,
                          const std::function<bool(ExecutionLoop &,
                                                   TCPSocket &&)> & connection_callback );

  Poller & poller() { return poller_; }

  Poller::Result loop_once( const int timeout_ms = -1 );
};

#endif /* PBRT_EXECUTION_LOOP_H */
