%% Iris Erlang Binding
%% Copyright 2013 Peter Szilagyi. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% decentralized messaging framework, and as such, the same licensing terms
%% hold. For details please see http://github.com/karalabe/iris/LICENSE.md
%%
%% Author: peterke@gmail.com (Peter Szilagyi)

-module(broadcast_tests).
-include_lib("eunit/include/eunit.hrl").

-behaviour(iris_server).
-export([init/1, handle_broadcast/2, handle_request/3, handle_publish/3,
	handle_tunnel/2, handle_drop/2, terminate/2]).

%% Local Iris node's listener port
-define(RELAY_PORT, 55555).


%% =============================================================================
%% Tests
%% =============================================================================

%% Broadcasts to itself a handful of messages.
single_test() ->
	% Connect to the Iris network
	{ok, Server} = iris_server:start(?RELAY_PORT, "single", ?MODULE, self()),

	% Broadcast a handfull of messages to onself
	Count = 100,
	Messages = ets:new(broadcast, [set, private]),
	lists:foreach(fun(_) ->
		Msg = crypto:strong_rand_bytes(128),
		true = ets:insert_new(Messages, {Msg}),
		ok = iris_server:broadcast(Server, "single", Msg)
	end, lists:seq(1, Count)),

	% Retrieve and verify all broadcasts
	lists:foreach(fun(_) ->
		receive
			Msg ->
				[{Msg}] = ets:lookup(Messages, Msg),
				ets:delete(Messages, Msg)
		after
			100 -> throw(timeout)
		end
	end, lists:seq(1, Count)),

	% Close the Iris connection
	ok = iris_server:close(Server).

%% Starts a numbef of concurrent processes, each broadcasting to the whole pool.
multi_test() ->
	Servers = 25,
	Broadcasts = 25,

	% Start up the concurrent broadcasters
	lists:foreach(fun(_) ->
		Parent = self(),
		spawn(fun() ->
			% Start a single server and signal parent
			{ok, Server} = iris_server:start(?RELAY_PORT, "multi", ?MODULE, self()),
			Parent ! {ok, self()},

			% Wait for permission to continue
			receive
				cont -> ok
			after
				250 -> throw(cont_timeout)
			end,

			% Broadcast the whole group
			lists:foreach(fun(_) ->
				ok = iris_server:broadcast(Server, "multi", <<"BROADCAST">>)
			end, lists:seq(1, Broadcasts)),

			% Retrieve and verify all broadcasts
			lists:foreach(fun(_) ->
				receive
					<<"BROADCAST">> -> ok
				after
					250 -> throw(bcast_timeout)
				end
			end, lists:seq(1, Servers * Broadcasts)),

			% Terminate the server and signal parent
			ok = iris_server:close(Server),
			Parent ! done
		end)
	end, lists:seq(1, Servers)),

	% Wait for all the inits
	Pids = lists:map(fun(_) ->
		receive
			{ok, Pid} -> Pid
		after
			250 -> throw(start_timeout)
		end
	end, lists:seq(1, Servers)),

	% Permit all servers to begin broadcast
	lists:foreach(fun(Pid) -> Pid ! cont end, Pids),

	% Wait for all the terminations (bit timeout)
	lists:foreach(fun(_) ->
		receive
			done -> ok
		after
			1000 -> throw(close_timeout)
		end
	end, lists:seq(1, Servers)).


%% =============================================================================
%% Iris server callback methods
%% =============================================================================

%% Simply saves the parent tester for reporting events.
init(Parent) -> {ok, Parent}.

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

handle_publish(_Topic, _Event, State) ->
	{stop, unimplemented, State}.

handle_tunnel(_Tunnel, State) ->
	{stop, unimplemented, State}.

handle_drop(_Reason, State) ->
	{stop, unimplemented, State}.
