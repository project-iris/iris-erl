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
	  	% Open a tunnel to the service cluster
	  	{ok, Tun} = iris_client:tunnel(Conn, ?CONFIG_CLUSTER, 1000),

	  	% Tear down the tunnel
      timer:sleep(1000),
	  	iris_tunnel:close(Tun),
      iris_barrier:exit(Barrier)
	  end)
	end, lists:seq(1, Tunnels)),

  % Schedule the parallel operations
  ok = iris_barrier:wait(Barrier).


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
	%case iris:recv(Tunnel, infinity) of
%		{ok, Message} ->
%			ok = iris:send(Tunnel, Message, infinity),
%			echoer(Tunnel);
%		{error, _Reason} ->
%			ok = iris:close(Tunnel)
%	end.
	iris_tunnel:close(Tunnel).

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
