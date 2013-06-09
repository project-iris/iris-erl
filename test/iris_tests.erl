%% Iris Erlang Binding
%% Copyright 2013 Peter Szilagyi. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% decentralized messaging framework, and as such, the same licensing terms
%% hold. For details please see http://github.com/karalabe/iris/LICENSE.md
%%
%% Author: peterke@gmail.com (Peter Szilagyi)

-module(iris_tests).
-export([broadcast_inst/2, reqrep_req/4, reqrep_rep/2, pubsub_inst/3]).
-include_lib("eunit/include/eunit.hrl").

%% =============================================================================
%% Connection setup and teardown tests
%% =============================================================================

%% Creates a handful of concurrent connection, then tears them down.
basics_test() ->
	Ids = lists:seq(0, 99),
	Apps = lists:map(fun(Id) ->	io_lib:format("basics-~p", [Id]) end, Ids),
	Conns = lists:map(fun connect/1, Apps),
	lists:map(fun close/1, Conns).

%% Connects to the Iris network.
connect(Id) ->
	Result = iris:connect(55555, Id),
	?assertMatch({ok, _Conn}, Result),

	% Extract and return the connection pid
	element(2, Result).

%% Disconnects from the Iris network.
close(Conn) ->
	?assertMatch(ok, iris:close(Conn)).


%% =============================================================================
%% Broadcasting tests
%% =============================================================================

%% Creates a handful of connections, broadcasts between each other, repeats.
broadcast_test() ->
	Reps = lists:seq(0, 10),
	Apps = lists:map(fun(Id) ->	io_lib:format("broadcast-~p", [Id]) end, Reps),
	lists:map(fun(Id) -> broadcast_suite(Id, 100) end, Apps).

%% Runs a single broadcast test suite, spawning a number of processes that send
%% each other the application id.
broadcast_suite(Id, Instances) ->
	% Start up N-1 concurrent processes
	lists:foreach(fun(_) ->
		spawn(?MODULE, broadcast_inst, [Id, Instances])
	end, lists:seq(0, Instances-2)),

	% Start the last synced
	broadcast_inst(Id, Instances).

%% Executes a single instance of the broadcast server.
broadcast_inst(Id, Instances) ->
	% Connect to the Iris network
	Conn = connect(Id),

	% Sleep a while to wait for all other connecting instances
	timer:sleep(50),

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


%% =============================================================================
%% Request/Reply tests
%% =============================================================================

%% Creates two request/reply processes, one issuing random requests, the other
%% echoing back everything. The tests are repeated a few times.
reqrep_test() ->
	Reps = lists:seq(0, 9),
	Apps = lists:map(fun(Id) ->	io_lib:format("reqrep-~B", [Id]) end, Reps),
	lists:map(fun(Id) -> reqrep_suite(Id, 10, 10) end, Apps).

%% Runs a single req/rep test suite. See above for details.
reqrep_suite(Id, Concurrent, Exchanges) ->
	% Start up N concurrent req/rep pairs
	lists:foreach(fun(Idx) ->
		SubId = io_lib:format("~s-~B", [Id, Idx]),
		spawn(?MODULE, reqrep_req, [Id, SubId, Exchanges, self()]),
		spawn(?MODULE, reqrep_rep, [SubId, Exchanges])
	end, lists:seq(0, Concurrent-1)),

	% Wait for all the children to terminate
	lists:foreach(fun(_) ->
		receive
			done -> ok
		after
			5000 -> throw(timeout)
		end
	end, lists:seq(0, Concurrent-1)).

%% Requester part of the exchange: connects to the Iris network and issues a few
%% concurrent requests to its pair process, waiting for the replies.
reqrep_req(Id, App, Exchanges, Parent) ->
	% Connect to the Iris network
	Conn = connect(Id),

	% Sleep a while to wait for all other connecting instances
	timer:sleep(50),

	% Issue the concurrent requests
	lists:foreach(fun(Idx) ->
		Owner = self(),
		spawn(fun() ->
			?assertMatch({ok, <<Idx>>}, iris:request(Conn, App, <<Idx>>, 1000)),
			Owner ! done
		end)
	end, lists:seq(0, Exchanges-1)),

	% Wait for all the children to terminate
	lists:foreach(fun(_) ->
		receive
			done -> ok
		after
			3000 -> throw(timeout)
		end
	end, lists:seq(0, Exchanges-1)),

	% Tear down the connection
	close(Conn),

	% Notify the parent of completion
	Parent ! done.

%% The replies pair of the exchange: connects to the Iris network and echoes
%% back a handful of requests.
reqrep_rep(Id, Exchanges) ->
	% Connect to the Iris network
	Conn = connect(Id),

	% Reply to all the required requests
	lists:foreach(fun(_) ->
		receive
			{request, From, Req} ->
				?assert(ok =:= iris:reply(From, Req))
		after
			3000 -> throw(timeout)
		end
	end, lists:seq(0, Exchanges-1)),

  % TODO: Temp fix for a minor race condition (reply and immediate close)
	timer:sleep(50),

	% Tear down the connection
	close(Conn).


%% =============================================================================
%% Publish/Subscribe tests
%% =============================================================================

%% Creates a handfull of connections, all subscribe to the same topic, each
%% publishing a few events. If all goes well, repeat.
pubsub_test() ->
	Reps = lists:seq(0, 9),
	Apps = lists:map(fun(Id) ->	io_lib:format("pubsub-~B", [Id]) end, Reps),
	lists:map(fun(Id) -> pubsub_suite(Id, 10, 10) end, Apps).

pubsub_suite(Id, Instances, Events) ->
	% Start up N-1 concurrent processes
	lists:foreach(fun(_) ->
		spawn(?MODULE, pubsub_inst, [Id, Instances, Events])
	end, lists:seq(0, Instances-2)),

	% Start the last synced
	pubsub_inst(Id, Instances, Events).

%% Executes a single instance of the pubsub server.
pubsub_inst(Id, Instances, Events) ->
	% Connect to the Iris network and subscribe to the same topic
	Conn = connect(Id),
	?assertMatch(ok, iris:subscribe(Conn, Id)),

	% Sleep a while to wait for all other connecting instances
	timer:sleep(50),

	% Publish all events
	lists:foreach(fun(_) ->
		?assertMatch(ok, iris:publish(Conn, Id, list_to_binary(Id)))
	end, lists:seq(0, Events-1)),

	% Wait for all events
	lists:foreach(fun(_) ->
		receive
			Msg ->
				% Make sure format and content match requirements
				?assertMatch({publish, _, _}, Msg),
				?assert(lists:flatten(Id) =:= element(2, Msg)),
				?assert(list_to_binary(Id) =:= element(3, Msg))
		after
			1000 -> throw(timeout)
		end
	end, lists:seq(0, Instances*Events - 1)),

	% Unsubscribe and wait for others to do the same
	?assertMatch(ok, iris:unsubscribe(Conn, Id)),
	timer:sleep(50),

	% Make sure further events don't get through
	?assertMatch(ok, iris:publish(Conn, Id, list_to_binary(Id))),
	receive
		_ -> ?assert(false)
	after
		100 -> ok
	end,

	% Tear down the connection
	close(Conn).
