%% Iris Erlang binding
%% Copyright (c) 2013 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

-module(reqrep_tests).
-include_lib("eunit/include/eunit.hrl").
-include("configs.hrl").

-behaviour(iris_server).
-export([init/2, handle_broadcast/2, handle_request/3, handle_tunnel/3,
  handle_drop/2, terminate/2]).

%% Tests multiple concurrent client and service requests.
request_test() ->
  % Test specific configurations
  ConfClients  = 25,
  ConfServers  = 25,
  ConfRequests = 25,

  Barrier = iris_barrier:new(ConfClients + ConfServers),

  % Start up the concurrent requesting clients
  lists:foreach(fun(Client) ->
    spawn(fun() ->
      try
        % Connect to the local relay
        {ok, Conn} = iris_client:start(?CONFIG_RELAY),
        iris_barrier:sync(Barrier),

        % Request from the service cluster
        lists:foreach(fun(Index) ->
          Request = io_lib:format("client #~p, request ~p", [Client, Index]),
          Binary  = list_to_binary(Request),
          {ok, Binary} = iris_client:request(Conn, ?CONFIG_CLUSTER, Binary, 1000)
        end, lists:seq(1, ConfRequests)),
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

        % Request from the service cluster
        lists:foreach(fun(Index) ->
          Request = io_lib:format("server #~p, request ~p", [Service, Index]),
          Binary  = list_to_binary(Request),
          {ok, Binary} = iris_client:request(Conn, ?CONFIG_CLUSTER, Binary, 1000)
        end, lists:seq(1, ConfRequests)),
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


%% Tests the request memory limitation.
request_memory_limit_test() ->
  % Register a new service to the relay
  {ok, Server} = iris_server:start(?CONFIG_RELAY, ?CONFIG_CLUSTER, ?MODULE, self(), [{request_memory, 1}]),
  Conn = receive
    {ok, Client} -> Client
  end,

  % Check that a 1 byte request passes
  {ok, _Reply} = iris_client:request(Conn, ?CONFIG_CLUSTER, <<0:8>>, 1),

  % Check that a 2 byte request is dropped
  {error, timeout} = iris_client:request(Conn, ?CONFIG_CLUSTER, <<0:8, 1:8>>, 1),

  % Check that space freed gets replenished
  {ok, _Reply} = iris_client:request(Conn, ?CONFIG_CLUSTER, <<0:8>>, 1),

  % Unregister the service
  ok = iris_server:stop(Server).


%% =============================================================================
%% Iris server callback methods
%% =============================================================================

%% Simply saves the parent tester for reporting events.
init(Conn, Parent) ->
  Parent ! {ok, Conn},
  {ok, sync}.

%% Echoes a request back to the sender. Depending on the parity of the request,
%% the reply is sent back sync or async.
handle_request(Request, _From, sync) ->
	{reply, {ok, Request}, async};

handle_request(Request, From, async) ->
	spawn(fun() ->
		ok = iris_server:reply(From, {ok, Request})
	end),
	{noreply, sync}.

%% No state to clean up.
terminate(_Reason, _State) -> ok.


%% =============================================================================
%% Unused Iris server callback methods (shuts the compiler up)
%% =============================================================================

handle_broadcast(_Message, State) ->
	{stop, unimplemented, State}.

handle_tunnel(_Tunnel, State, _Link) ->
	{stop, unimplemented, State}.

handle_drop(_Reason, State) ->
	{stop, unimplemented, State}.
