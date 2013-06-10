%% Iris Erlang Binding
%% Copyright 2013 Peter Szilagyi. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% decentralized messaging framework, and as such, the same licensing terms
%% hold. For details please see http://github.com/karalabe/iris/LICENSE.md
%%
%% Author: peterke@gmail.com (Peter Szilagyi)

-module(connection_tests).
-include_lib("eunit/include/eunit.hrl").

-behaviour(iris_server).
-export([init/1, handle_broadcast/2, handle_request/3, handle_publish/3,
	handle_tunnel/2, handle_drop/2, terminate/2]).

%% Local Iris node's listener port
-define(RELAY_PORT, 55555).


%% =============================================================================
%% Tests
%% =============================================================================

%% Starts a single Iris server and checks startup and shutdown.
single_test() ->
	% Start the server, checking success and init call
	{ok, Server} = iris_server:start(?RELAY_PORT, "single", ?MODULE, self()),
	receive
		init -> ok
	after
		250 -> throw(init_timeout)
	end,

	% Terminate the server, checking success and terminate call
	ok = iris_server:close(Server),
	receive
		terminate -> ok
	after
		250 -> throw(term_timeout)
	end.

%% Starts a handful of concurrent Iris servers and checks startup and shutdown.
multi_test() ->
	% Number of concurrent connections to initiate
	Count = 100,

	% Start the servers concurrently
	lists:foreach(fun(_) ->
		Parent = self(),
		spawn(fun() ->
			% Start a single server and signal parent
			{ok, Server} = iris_server:start(?RELAY_PORT, "multi", ?MODULE, self()),
			receive
				init -> Parent ! {ok, self()}
			after
				250 -> throw(init_timeout)
			end,

			% Wait for permission to continue
			receive
				cont -> ok
			after
				250 -> throw(cont_timeout)
			end,

			% Terminate the server and signal parent
			ok = iris_server:close(Server),
			receive
				terminate -> Parent ! done
			after
				250 -> throw(term_timeout)
			end
		end)
	end, lists:seq(1, Count)),

	% Wait for all the inits
	Pids = lists:map(fun(_) ->
		receive
			{ok, Pid} -> Pid
		after
			250 -> throw(start_timeout)
		end
	end, lists:seq(1, Count)),

	% Permit all servers to shut down
	lists:foreach(fun(Pid) -> Pid ! cont end, Pids),

	% Wait for all the terminations
	lists:foreach(fun(_) ->
		receive
			done -> ok
		after
			250 -> throw(close_timeout)
		end
	end, lists:seq(1, Count)).


%% =============================================================================
%% Iris server callback methods
%% =============================================================================

%% Notifies the tester of the successful init call.
init(Parent) ->
	Parent ! init,
	{ok, {state, Parent}}.

%% Notifies the tester of the successful terminate call.
terminate(_Reason, {state, Parent}) ->
	Parent ! terminate.


%% =============================================================================
%% Unused Iris server callback methods (shuts the compiler up)
%% =============================================================================

handle_broadcast(_Message, State) ->
	{stop, unimplemented, State}.

handle_request(_Request, _From, State) ->
	{stop, unimplemented, State}.

handle_publish(_Topic, _Event, State) ->
	{stop, unimplemented, State}.

handle_tunnel(_Tunnel, State) ->
	{stop, unimplemented, State}.

handle_drop(_Reason, State) ->
	{stop, unimplemented, State}.
