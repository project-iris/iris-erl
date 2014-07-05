%% Iris Erlang binding
%% Copyright (c) 2013 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

-module(reqrep_time_tests).
-include_lib("eunit/include/eunit.hrl").
-include("configs.hrl").

-behaviour(iris_server).
-export([init/2, handle_broadcast/2, handle_request/3, handle_tunnel/3,
  handle_drop/2, terminate/2]).


%% Tests the request timeouts.
request_timeout_test() ->
  % Test specific configurations
  ConfSleep = 25,

  % Register a new service to the relay
  {ok, Server} = iris_server:start(?CONFIG_RELAY, ?CONFIG_CLUSTER, ?MODULE, {self(), ConfSleep}),
  Conn = receive
    {ok, Client} -> Client
  end,

  % Check that the timeouts are complied with
  {ok, _Reply}     = iris_client:request(Conn, ?CONFIG_CLUSTER, <<0:8>>, ConfSleep * 2),
  {error, timeout} = iris_client:request(Conn, ?CONFIG_CLUSTER, <<0:8>>, ConfSleep div 2),

  % Flush all received messages (eunit reuses tester processes)
  flush(),

  % Unregister the service
  ok = iris_server:stop(Server).


%% Tests that enqueued but expired requests don't get executed
request_expiration_test() ->
  % Test specific configurations
  ConfRequests = 4,
  ConfSleep    = 25,

  % Register a new service to the relay
  {ok, Server} = iris_server:start(?CONFIG_RELAY, ?CONFIG_CLUSTER, ?MODULE, {self(), ConfSleep}),
  Conn = receive
    {ok, Client} -> Client
  end,

  % Start a batch of concurrent requesters (all but one should be scheduled remotely)
  lists:foreach(fun(Index) ->
    spawn(iris_client, request, [Conn, ?CONFIG_CLUSTER, <<Index:8>>, 1])
  end, lists:seq(1, ConfRequests)),

  % Wait for all of them to complete and verify that all but 1 expired
  timer:sleep((ConfRequests + 1) * ConfSleep),

  ok = receive
    _ -> ok
  after
    1 -> timeout
  end,

  ok = receive
    _ -> not_expired
  after
    1 -> ok
  end,

  % Unregister the service
  ok = iris_server:stop(Server).


% Flushes all messages from the process mailbox.
flush() ->
  receive
    _ -> flush()
  after
    1 -> ok
  end.


%% =============================================================================
%% Iris server callback methods
%% =============================================================================

%% Simply saves the parent tester for reporting events.
init(Conn, {Parent, Sleep}) ->
  Parent ! {ok, Conn},
  {ok, {Parent, Sleep}}.

%% Sleeps a while and echoes the request back to the sender.
handle_request(Request, _From, {Parent, Sleep}) ->
  Parent ! Request,
  timer:sleep(Sleep),
  {reply, {ok, Request}, {Parent, Sleep}}.

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
