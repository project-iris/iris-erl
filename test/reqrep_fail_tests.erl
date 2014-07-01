%% Iris Erlang binding
%% Copyright (c) 2013 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

-module(reqrep_fail_tests).
-include_lib("eunit/include/eunit.hrl").
-include("configs.hrl").

-behaviour(iris_server).
-export([init/2, handle_broadcast/2, handle_request/3, handle_tunnel/3,
  handle_drop/2, terminate/2]).


%% Tests request failure forwarding.
request_fail_test() ->
  % Test specific configurations
  ConfRequests = 125,

  % Register a new service to the relay
  {ok, Server} = iris_server:start_link(?CONFIG_RELAY, ?CONFIG_CLUSTER, ?MODULE, self()),
  Conn = receive
    {ok, Client} -> Client
  end,

  % Request from the failing service cluster
  lists:foreach(fun(Index) ->
    Request = lists:flatten(io_lib:format("failure ~p", [Index])),
    Binary  = list_to_binary(Request),
    {error, Request} = iris_client:request(Conn, ?CONFIG_CLUSTER, Binary, 1000)
  end, lists:seq(1, ConfRequests)),

  % Unregister the service
  ok = iris_server:stop(Server).


%% =============================================================================
%% Iris server callback methods
%% =============================================================================

%% Simply saves the parent tester for reporting events.
init(Conn, Parent) ->
  Parent ! {ok, Conn},
  {ok, sync}.

%% Echoes a request back to the sender, but on the error channel. Depending on
%% the parity of the request, the reply is sent back sync or async.
handle_request(Request, _From, sync) ->
  {reply, {error, Request}, async};

handle_request(Request, From, async) ->
  spawn(fun() ->
    ok = iris_server:reply(From, {error, Request})
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
