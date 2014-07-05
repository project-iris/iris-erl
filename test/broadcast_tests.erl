%% Iris Erlang binding
%% Copyright (c) 2013 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

-module(broadcast_tests).
-include_lib("eunit/include/eunit.hrl").
-include("configs.hrl").

-behaviour(iris_server).
-export([init/2, handle_broadcast/2, handle_request/3,	handle_tunnel/3,
	handle_drop/2, terminate/2]).


%% Tests multiple concurrent client and service broadcasts.
broadcast_test() ->
  % Test specific configurations
	ConfClients  = 25,
	ConfServers  = 25,
	ConfMessages = 25,

	Barrier = iris_barrier:new(ConfClients + ConfServers),

	% Start up the concurrent broadcasting clients
	lists:foreach(fun(Client) ->
		spawn(fun() ->
			try
				% Connect to the local relay
				{ok, Conn} = iris_client:start(?CONFIG_RELAY),
				iris_barrier:sync(Barrier),

				% Broadcast to the whole service cluster
				lists:foreach(fun(Index) ->
					Message = io_lib:format("client #~p, broadcast ~p", [Client, Index]),
					ok = iris_client:broadcast(Conn, ?CONFIG_CLUSTER, list_to_binary(Message))
				end, lists:seq(1, ConfMessages)),
				iris_barrier:sync(Barrier),

				% Disconnect from the local relay
				ok = iris_client:stop(Conn),
				iris_barrier:exit(Barrier)
      catch
        error:Exception -> iris_barrier:exit(Barrier, Exception)
      end
		end)
	end, lists:seq(1, ConfClients)),

	% Start up the concurrent broadcast services
	lists:foreach(fun(Service) ->
		spawn(fun() ->
			try
				% Register a new service to the relay
				{ok, Server} = iris_server:start(?CONFIG_RELAY, ?CONFIG_CLUSTER, ?MODULE, self()),
				Conn = receive
					{ok, Client} -> Client
				end,
				iris_barrier:sync(Barrier),

				% Broadcast to the whole service cluster
				lists:foreach(fun(Index) ->
					Message = io_lib:format("server #~p, broadcast ~p", [Service, Index]),
					ok = iris_client:broadcast(Conn, ?CONFIG_CLUSTER, list_to_binary(Message))
				end, lists:seq(1, ConfMessages)),
				iris_barrier:sync(Barrier),

				% Retrieve all the arrived broadcasts
				Messages = ets:new(messages, [set, private]),
				lists:foreach(fun(_) ->
					receive
						Binary -> ets:insert_new(Messages, {Binary})
					end
				end, lists:seq(1, (ConfClients + ConfServers) * ConfMessages)),

				% Verify all the individual broadcasts
				lists:foreach(fun(ClientId) ->
					lists:foreach(fun(Index) ->
						Message = io_lib:format("client #~p, broadcast ~p", [ClientId, Index]),
						Binary  = list_to_binary(Message),
						true = ets:member(Messages, Binary),
						true = ets:delete(Messages, Binary)
					end, lists:seq(1, ConfMessages))
				end, lists:seq(1, ConfClients)),

				lists:foreach(fun(ServiceId) ->
					lists:foreach(fun(Index) ->
						Message = io_lib:format("server #~p, broadcast ~p", [ServiceId, Index]),
						Binary  = list_to_binary(Message),
						true = ets:member(Messages, Binary),
						true = ets:delete(Messages, Binary)
					end, lists:seq(1, ConfMessages))
				end, lists:seq(1, ConfServers)),

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
	ok = iris_barrier:wait(Barrier),
	ok = iris_barrier:wait(Barrier).


%% Tests the broadcast memory limitation.
broadcast_memory_limit_test() ->
  % Register a new service to the relay
  {ok, Server} = iris_server:start(?CONFIG_RELAY, ?CONFIG_CLUSTER, ?MODULE, self(), [{broadcast_memory, 1}]),
  Conn = receive
    {ok, Client} -> Client
  end,

  % Check that a 1 byte broadcast passes
  ok = iris_client:broadcast(Conn, ?CONFIG_CLUSTER, <<0:8>>),
  ok = receive
    _ -> ok
  after
    1 -> timeout
  end,

  % Check that a 2 byte broadcast is dropped
  ok = iris_client:broadcast(Conn, ?CONFIG_CLUSTER, <<0:8, 1:8>>),
  ok = receive
    _ -> not_dropped
  after
    1 -> ok
  end,

  % Check that space freed gets replenished
  ok = iris_client:broadcast(Conn, ?CONFIG_CLUSTER, <<0:8>>),
  ok = receive
    _ -> ok
  after
    1 -> timeout
  end,

  % Unregister the service
  ok = iris_server:stop(Server).


%% =============================================================================
%% Iris server callback methods
%% =============================================================================

%% Simply saves the parent tester for reporting events.
init(Conn, Parent) ->
	Parent ! {ok, Conn},
	{ok, Parent}.

%% Handles a broadcast event by reporting it to the tester process.
handle_broadcast(Message, Parent) ->
	Parent ! Message,
	{noreply, Parent}.

%% No state to clean up.
terminate(_Reason, _State) -> ok.


%% =============================================================================
%% Unused Iris server callback methods (shuts the compiler up)
%% =============================================================================

handle_request(_Request, _From, State) ->
	{stop, unimplemented, State}.

handle_tunnel(_Tunnel, State, _Link) ->
	{stop, unimplemented, State}.

handle_drop(_Reason, State) ->
	{stop, unimplemented, State}.
