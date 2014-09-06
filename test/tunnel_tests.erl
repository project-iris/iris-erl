%% Iris Erlang binding
%% Copyright (c) 2013 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

-module(tunnel_tests).
-include_lib("eunit/include/eunit.hrl").
-include("configs.hrl").

-behaviour(iris_server).
-export([init/2, handle_broadcast/2, handle_request/3, handle_tunnel/2,
	handle_drop/2, terminate/2]).


%% =============================================================================
%% Tests
%% =============================================================================

%% Tests multiple concurrent client and service tunnels.
tunnel_test() ->
  % Test specific configurations
  ConfClients   = 7,
  ConfServers   = 7,
  ConfTunnels   = 7,
  ConfExchanges = 7,

  Barrier = iris_barrier:new(ConfClients + ConfServers),

  % Start up the concurrent requesting clients
  lists:foreach(fun(Client) ->
    spawn(fun() ->
      try
        % Connect to the local relay
        {ok, Conn} = iris_client:start(?CONFIG_RELAY),
        iris_barrier:sync(Barrier),

        % Execute the tunnel construction, message exchange and verification
        Id = io_lib:format("client #~p", [Client]),
        ok = build_exchange_verify(Id, Conn, ConfTunnels, ConfExchanges),
        iris_barrier:sync(Barrier),

        % Disconnect from the local relay
        ok = iris_client:stop(Conn),
        iris_barrier:exit(Barrier)
      catch
        error:Exception -> iris_barrier:exit(Barrier, Exception)
      end
    end)
  end, lists:seq(1, ConfClients)),

  % Start up the concurrent request services
  lists:foreach(fun(Service) ->
    spawn(fun() ->
      try
        % Register a new service to the relay
        {ok, Server} = iris_server:start(?CONFIG_RELAY, ?CONFIG_CLUSTER, ?MODULE, self()),
        Conn = receive
          {ok, Client} -> Client
        end,
        iris_barrier:sync(Barrier),

        % Execute the tunnel construction, message exchange and verification
        Id = io_lib:format("server #~p", [Service]),
        ok = build_exchange_verify(Id, Conn, ConfTunnels, ConfExchanges),
        iris_barrier:sync(Barrier),

        % Unregister the service
        ok = iris_server:stop(Server),
        iris_barrier:exit(Barrier)
      catch
        error:Exception -> iris_barrier:exit(Barrier, Exception)
      end
    end)
  end, lists:seq(1, ConfServers)),

  % Schedule the parallel operations
  ok = iris_barrier:wait(Barrier),
  ok = iris_barrier:wait(Barrier),
  ok = iris_barrier:wait(Barrier).


build_exchange_verify(Id, Conn, Tunnels, Exchanges) ->
  Barrier = iris_barrier:new(Tunnels),
  lists:foreach(fun(Tunnel) ->
	  spawn(fun() ->
      try
  	  	% Open a tunnel to the service cluster
  	  	{ok, Tun} = iris_client:tunnel(Conn, ?CONFIG_CLUSTER, 1000),

        % Serialize a batch of messages
        lists:foreach(fun(Index) ->
          Message = io_lib:format("~s, tunnel #~p, message #~p", [Id, Tunnel, Index]),
          Binary  = list_to_binary(Message),
          ok = iris_tunnel:send(Tun, Binary, 1000)
        end, lists:seq(1, Exchanges)),

        % Read back the echo stream and verify
        lists:foreach(fun(Index) ->
          Message = io_lib:format("~s, tunnel #~p, message #~p", [Id, Tunnel, Index]),
          Binary  = list_to_binary(Message),
          {ok, Binary} = iris_tunnel:recv(Tun, 1000)
        end, lists:seq(1, Exchanges)),

  	  	% Tear down the tunnel
  	  	ok = iris_tunnel:close(Tun)
      catch
        error:Exception -> iris_barrier:exit(Barrier, Exception)
      end,
      iris_barrier:exit(Barrier)
	  end)
	end, lists:seq(1, Tunnels)),

  % Schedule the parallel operations
  ok = iris_barrier:wait(Barrier).


%% Tests that unanswered tunnels timeout correctly.
tunnel_timeout_test() ->
  % Connect to the local relay
  {ok, Conn} = iris_client:start(?CONFIG_RELAY),

  % Open a new tunnel to a non existent server
  {error, timeout} = iris_conn:tunnel(Conn, ?CONFIG_CLUSTER, 100),

  % Disconnect from the local relay
  ok = iris_client:stop(Conn).


%% Tests that large messages get delivered properly.
tunnel_chunking_test() ->
  % Register a new service to the relay
  {ok, Server} = iris_server:start(?CONFIG_RELAY, ?CONFIG_CLUSTER, ?MODULE, self()),
  Conn = receive
    {ok, Client} -> Client
  end,

  % Construct the tunnel
  {ok, Tunnel} = iris_conn:tunnel(Conn, ?CONFIG_CLUSTER, 1000),

  % Create and transfer a huge message
  Blob = list_to_binary([
  	[X rem 256, X rem 256, X rem 256, X rem 256, X rem 256, X rem 256, X rem 256, X rem 256]
  	|| X <- lists:seq(1, 1024*1024)]
  ),
  ok         = iris_tunnel:send(Tunnel, Blob, 10000),
  {ok, Blob} = iris_tunnel:recv(Tunnel, 10000),

	% Tear down the tunnel
	ok = iris_tunnel:close(Tunnel),

  % Unregister the service
  ok = iris_server:stop(Server).


%% Tests that a tunnel remains operational even after overloads (partially
%% transferred huge messages timeouting).
tunnel_overload_test() ->
  % Register a new service to the relay
  {ok, Server} = iris_server:start(?CONFIG_RELAY, ?CONFIG_CLUSTER, ?MODULE, self()),
  Conn = receive
    {ok, Client} -> Client
  end,

  % Construct the tunnel
  {ok, Tunnel} = iris_conn:tunnel(Conn, ?CONFIG_CLUSTER, 1000),

  % Overload the tunnel by partially transferring huge messages
  Blob = list_to_binary([
  	[X rem 256, X rem 256, X rem 256, X rem 256, X rem 256, X rem 256, X rem 256, X rem 256]
  	|| X <- lists:seq(1, 1024*1024)]
  ),
  lists:foreach(fun(_) ->
		{error, timeout} = iris_tunnel:send(Tunnel, Blob, 10)
  end, lists:seq(1, 10)),

  % Verify that the tunnel is still operational
  Data = list_to_binary([0, 1, 0, 2, 0, 3, 0, 4]),
  lists:foreach(fun(_) -> % Iteration's important, the first will always cross (allowance ignore)
		ok         = iris_tunnel:send(Tunnel, Data, 1000),
		{ok, Data} = iris_tunnel:recv(Tunnel, 1000)
  end, lists:seq(1, 10)),

	% Tear down the tunnel
	ok = iris_tunnel:close(Tunnel),

  % Unregister the service
  ok = iris_server:stop(Server).


%% Benchmarks the latency of a single tunnel send (actually two way, so halves it).
tunnel_latency_benchmark_test() ->
  % Register a new service to the relay
  {ok, Server} = iris_server:start(?CONFIG_RELAY, ?CONFIG_CLUSTER, ?MODULE, self()),
  Conn = receive
    {ok, Client} -> Client
  end,

  % Construct the tunnel
  {ok, Tunnel} = iris_conn:tunnel(Conn, ?CONFIG_CLUSTER, 1000),

  % Measure the latency
	benchmark:iterated(?FUNCTION, 5000, fun() ->
		ok      = iris_tunnel:send(Tunnel, <<0:8>>, 1000),
		{ok, _} = iris_tunnel:recv(Tunnel, 1000),
		ok
 	end, 0.5),

	% Tear down the tunnel
	ok = iris_tunnel:close(Tunnel),

  % Unregister the service
  ok = iris_server:stop(Server).


%% Benchmarks the throughput of a single tunnel send (actually two way, so halves it).
tunnel_throughput_benchmark_test() ->
  % Register a new service to the relay
  {ok, Server} = iris_server:start(?CONFIG_RELAY, ?CONFIG_CLUSTER, ?MODULE, self()),
  Conn = receive
    {ok, Client} -> Client
  end,

  % Construct the tunnel
  {ok, Tunnel} = iris_conn:tunnel(Conn, ?CONFIG_CLUSTER, 1000),

  % Measure the throughput
  Map = fun() ->
  	ok = iris_tunnel:send(Tunnel, <<0:8>>, 1000)
  end,
  Reduce = fun() ->
		{ok, _} = iris_tunnel:recv(Tunnel, 1000),
		ok
	end,

	benchmark:threaded(?FUNCTION, 1, 5000, Map, Reduce, 0.5),

	% Tear down the tunnel
	ok = iris_tunnel:close(Tunnel),

  % Unregister the service
  ok = iris_server:stop(Server).


%% =============================================================================
%% Iris server callback methods
%% =============================================================================

%% Simply saves the parent tester for reporting events.
%% Simply saves the parent tester for reporting events.
init(Conn, Parent) ->
  Parent ! {ok, Conn},
  {ok, nil}.

%% Spawns a new process that echoes back all tunnel data.
handle_tunnel(Tunnel, State) ->
	spawn(fun() -> echoer(Tunnel) end),
	{noreply, State}.

echoer(Tunnel) ->
	case iris_tunnel:recv(Tunnel, infinity) of
		{ok, Message} ->
			ok = iris_tunnel:send(Tunnel, Message, infinity),
			echoer(Tunnel);
		{error, closed} ->
			ok = iris_tunnel:close(Tunnel)
	end.

%% No state to clean up.
terminate(_Reason, _State) -> ok.


%% =============================================================================
%% Unused Iris server callback methods (shuts the compiler up)
%% =============================================================================

handle_broadcast(_Message, State) ->
	{stop, unimplemented, State}.

handle_request(_Request, _From, State) ->
	{stop, unimplemented, State}.

handle_drop(_Reason, State) ->
	{stop, unimplemented, State}.
