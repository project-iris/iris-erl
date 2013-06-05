%% Iris Erlang Binding
%% Copyright 2013 Peter Szilagyi. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% decentralized messaging framework, and as such, the same licensing terms
%% hold. For details please see http://github.com/karalabe/iris/LICENSE.md
%%
%% Author: peterke@gmail.com (Peter Szilagyi)

-module(iris_basics_tests).
-export([broadcast_inst/2]).
-include_lib("eunit/include/eunit.hrl").

%% =============================================================================
%% Connection setup and teardown tests
%% =============================================================================

% Create a handful of concurrent connection, then tear them down
basics_test() ->
	Ids = lists:seq(0, 99),
	Apps = lists:map(fun(Id) ->	io_lib:format("basics-~p", [Id]) end, Ids),
	Conns = lists:map(fun connect/1, Apps),
	lists:map(fun close/1, Conns).

% Connect to the Iris network
connect(Id) ->
	Result = iris:connect(55555, Id),
	?assertMatch({ok, _Conn}, Result),

	% Extract and return the connection pid
	element(2, Result).

% Disconnect from the Iris network
close(Conn) ->
	?assertMatch(ok, iris:close(Conn)).

%% =============================================================================
%% Broadcasting tests
%% =============================================================================

% Create a handful of connections, broadcast between each other, repeat.
broadcast_test() ->
	Reps = lists:seq(0, 10),
	Apps = lists:map(fun(Id) ->	io_lib:format("broadcast-~p", [Id])	end, Reps),
	lists:map(fun(Id) -> broadcast_suite(Id, 10) end, Apps).

broadcast_suite(_Id, 0) -> ok;
broadcast_suite(Id, Instances) ->
	% Start up N-1 concurrent processes
	lists:foreach(fun(_) ->
		spawn(?MODULE, broadcast_inst, [Id, Instances])
	end, lists:seq(0, Instances-2)),

	% Start the last synced
	broadcast_inst(Id, Instances).

broadcast_inst(Id, Instances) ->
	% Connect to the Iris network
	Conn = connect(Id),

	% Sleep a while to wait for all other connecting instances
	timer:sleep(100),

	% Broadcast a message for all, wait for all
	?assertMatch(ok, iris:broadcast(Conn, Id, list_to_binary(Id))),
	lists:foreach(fun(_) ->
		receive
			Msg ->
				% Make sure format and content match requirements
				?assertMatch({broadcast, _}, Msg),
				?assert(list_to_binary(Id) =:= element(2, Msg))
		after
			1000 -> throw(timeout)
		end
	end, lists:seq(0, Instances-1)),

	% Tear down the connection
	close(Conn).
