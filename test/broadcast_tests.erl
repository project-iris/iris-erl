%% Iris Erlang binding
%% Copyright (c) 2013 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

-module(broadcast_tests).
-include_lib("eunit/include/eunit.hrl").

-behaviour(iris_server).
-export([init/1, handle_broadcast/3, handle_request/4, handle_publish/4,
	handle_tunnel/3, handle_drop/2, terminate/2]).

%% Local Iris node's listener port
-define(RELAY_PORT, 55555).


%% =============================================================================
%% Tests
%% =============================================================================

%% Broadcasts to itself a handful of messages.
single_test() ->
	% Connect to the Iris network
	{ok, Server, Link} = iris_server:start(?RELAY_PORT, "single", ?MODULE, self()),

	% Broadcast a handfull of messages to onself
	Count = 1000,
	Messages = ets:new(broadcast, [set, private]),
	lists:foreach(fun(_) ->
		Msg = crypto:strong_rand_bytes(128),
		true = ets:insert_new(Messages, {Msg}),
		ok = iris:broadcast(Link, "single", Msg)
	end, lists:seq(1, Count)),

	% Retrieve and verify all broadcasts
	lists:foreach(fun(_) ->
		receive
			Msg ->
				[{Msg}] = ets:lookup(Messages, Msg),
				ets:delete(Messages, Msg)
		end
	end, lists:seq(1, Count)),

	% Close the Iris connection
	ok = iris_server:stop(Server).

%% Starts a numbef of concurrent processes, each broadcasting to the whole pool.
multi_test() ->
	Servers = 100,
	Broadcasts = 25,

	% Start up the concurrent broadcasters
	lists:foreach(fun(_) ->
		Parent = self(),
		spawn(fun() ->
			% Start a single server and signal parent
			{ok, Server, Link} = iris_server:start(?RELAY_PORT, "multi", ?MODULE, self()),
			Parent ! {ok, self()},

			% Wait for permission to continue
			receive
				cont -> ok
			end,

			% Broadcast the whole group
			lists:foreach(fun(_) ->
				ok = iris:broadcast(Link, "multi", <<"BROADCAST">>)
			end, lists:seq(1, Broadcasts)),

			% Retrieve and verify all broadcasts
			lists:foreach(fun(_) ->
				receive
					<<"BROADCAST">> -> ok
				end
			end, lists:seq(1, Servers * Broadcasts)),

			% Terminate the server and signal parent
			ok = iris_server:stop(Server),
			Parent ! done
		end)
	end, lists:seq(1, Servers)),

	% Wait for all the inits
	Pids = lists:map(fun(_) ->
		receive
			{ok, Pid} -> Pid
		end
	end, lists:seq(1, Servers)),

	% Permit all servers to begin broadcast
	lists:foreach(fun(Pid) -> Pid ! cont end, Pids),

	% Wait for all the terminations (big timeout)
	lists:foreach(fun(_) ->
		receive
			done -> ok
		end
	end, lists:seq(1, Servers)).


%% =============================================================================
%% Iris server callback methods
%% =============================================================================

%% Simply saves the parent tester for reporting events.
init(Parent) -> {ok, Parent}.

%% Handles a broadcast event by reporting it to the tester process.
handle_broadcast(Message, Parent, _Link) ->
	Parent ! Message,
	{noreply, Parent}.

%% No state to clean up.
terminate(_Reason, _State) -> ok.


%% =============================================================================
%% Unused Iris server callback methods (shuts the compiler up)
%% =============================================================================

handle_request(_Request, _From, State, _Link) ->
	{stop, unimplemented, State}.

handle_publish(_Topic, _Event, State, _Link) ->
	{stop, unimplemented, State}.

handle_tunnel(_Tunnel, State, _Link) ->
	{stop, unimplemented, State}.

handle_drop(_Reason, State) ->
	{stop, unimplemented, State}.
