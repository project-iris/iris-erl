%% Iris Erlang binding
%% Copyright (c) 2013 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

-module(tunnel_tests).
-include_lib("eunit/include/eunit.hrl").

-behaviour(iris_server).
-export([init/2, handle_broadcast/2, handle_request/4, handle_publish/4,
	handle_tunnel/3, handle_drop/2, terminate/2]).

%% Local Iris node's listener port
-define(RELAY_PORT, 55555).


%% =============================================================================
%% Tests
%% =============================================================================

% Opens a tunnel to itself and streams a batch of messages.
single_test() ->
	Messages = 1000,

	% Connect to the Iris network
	{ok, Server, Link} = iris_server:start(?RELAY_PORT, "single", ?MODULE, nil),

	% Open a tunnel to self
	{ok, Tunnel} = iris:tunnel(Link, "single", 500),

	% Serialize a load of messages
	lists:foreach(fun(Seq) ->
		ok = iris:send(Tunnel, <<Seq:32>>, 500)
	end, lists:seq(1, Messages)),

	% Read back the echo and verify
	lists:foreach(fun(Seq) ->
		{ok, <<Seq:32>>} = iris:recv(Tunnel, 500)
	end, lists:seq(1, Messages)),

	% Close the tunnel and the connection
	ok = iris:close(Tunnel),
	ok = iris_server:stop(Server).


% Starts a batch of servers, each sending and echoing a stream of messages.
multi_test() ->
	Servers = 75,
	Messages = 50,

	% Start up the concurrent tunnelers
	lists:foreach(fun(_) ->
		Parent = self(),
		spawn(fun() ->
			% Start a single server and signal parent
			{ok, Server, Link} = iris_server:start(?RELAY_PORT, "multi", ?MODULE, nil),
			Parent ! {ok, self()},

			% Wait for permission to continue
			receive
				cont -> ok
			end,

			% Open a tunnel to somebody in the group
			{ok, Tunnel} = iris:tunnel(Link, "multi", 1000),

			% Serialize a load of messages
			lists:foreach(fun(Seq) ->
				ok = iris:send(Tunnel, <<Seq:32>>, 1000)
			end, lists:seq(1, Messages)),

			% Read back the echo and verify
			lists:foreach(fun(Seq) ->
				{ok, <<Seq:32>>} = iris:recv(Tunnel, 1000)
			end, lists:seq(1, Messages)),

			% Close the tunnel
			ok = iris:close(Tunnel),

			% Signal the parent and wait for permission to terminate
			Parent ! done,
			receive
				term -> ok
			end,

			% Terminate the server and signal parent
			ok = iris_server:stop(Server),
			Parent ! stop
		end)
	end, lists:seq(1, Servers)),

	% Wait for all the inits and permit continues
	Pids = lists:map(fun(_) ->
		receive
			{ok, Pid} -> Pid
		end
	end, lists:seq(1, Servers)),
	lists:foreach(fun(Pid) -> Pid ! cont end, Pids),

	% Wait for all the servers to finish the tunnel tests and permit termination
	lists:foreach(fun(_) ->
		receive
			done -> ok
		end
	end, lists:seq(1, Servers)),
	lists:foreach(fun(Pid) -> Pid ! term end, Pids),

	% Wait for all the terminations
	lists:foreach(fun(_) ->
		receive
			stop -> ok
		end
	end, lists:seq(1, Servers)).

%% =============================================================================
%% Iris server callback methods
%% =============================================================================

%% Simply saves the parent tester for reporting events.
init(_Conn, nil) -> {ok, nil}.

%% Spawns a new process that echoes back all tunnel data.
handle_tunnel(Tunnel, State, _Link) ->
	spawn(fun() -> echoer(Tunnel) end),
	{noreply, State}.

echoer(Tunnel) ->
	case iris:recv(Tunnel, infinity) of
		{ok, Message} ->
			ok = iris:send(Tunnel, Message, infinity),
			echoer(Tunnel);
		{error, _Reason} ->
			ok = iris:close(Tunnel)
	end.

%% No state to clean up.
terminate(_Reason, _State) -> ok.


%% =============================================================================
%% Unused Iris server callback methods (shuts the compiler up)
%% =============================================================================

handle_broadcast(_Message, State) ->
	{stop, unimplemented, State}.

handle_request(_Request, _From, State, _Link) ->
	{stop, unimplemented, State}.

handle_publish(_Topic, _Event, State, _Link) ->
	{stop, unimplemented, State}.

handle_drop(_Reason, State) ->
	{stop, unimplemented, State}.
