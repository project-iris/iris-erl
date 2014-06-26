%% Iris Erlang binding
%% Copyright (c) 2013 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

-module(reqrep_tests).
-include_lib("eunit/include/eunit.hrl").

-behaviour(iris_server).
-export([init/2, handle_broadcast/2, handle_request/4, handle_publish/4,
	handle_tunnel/3, handle_drop/2, terminate/2]).

%% Local Iris node's listener port
-define(RELAY_PORT, 55555).


%% =============================================================================
%% Tests
%% =============================================================================

%% Sends a few requests to itself, waiting for the echo.
single_test() ->
	% Connect to the Iris network
	{ok, Server, Link} = iris_server:start(?RELAY_PORT, "single", ?MODULE, nil),

	% Send a handful of requests, verifying the replies
	Count = 1000,
	lists:foreach(fun(_) ->
		Req = crypto:strong_rand_bytes(128),
		{ok, Req} = iris:request(Link, "single", Req, 250)
	end, lists:seq(1, Count)),

	% Close the Iris connection
	ok = iris_server:stop(Server).

% Starts a handful of concurrent servers which send requests to each other.
multi_test() ->
	Servers = 75,
	Requests = 75,

	% Start up the concurrent requesters (and repliers)
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

			% Send the requests to the group and wait for the replies
			lists:foreach(fun(_) ->
				Req = crypto:strong_rand_bytes(128),
				{ok, Req} = iris:request(Link, "multi", Req, 250)
			end, lists:seq(1, Requests)),
			Parent ! done,

			% Wait for permission to terminate
			receive
				term -> ok
			end,

			% Terminate the server and signal parent
			ok = iris_server:stop(Server),
			Parent ! stop
		end)
	end, lists:seq(1, Servers)),

	% Wait for all the inits
	Pids = lists:map(fun(_) ->
		receive
			{ok, Pid} -> Pid
		end
	end, lists:seq(1, Servers)),

	% Permit all servers to begin the requests
	lists:foreach(fun(Pid) -> Pid ! cont end, Pids),

	% Wait for all the servers to finish the request tests
	lists:foreach(fun(_) ->
		receive
			done -> ok
		end
	end, lists:seq(1, Servers)),

	% Permit all servers to terminate
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
init(_Conn, nil) -> {ok, sync}.

%% Echoes a request back to the sender, notifying the parent of the event.
%% Depending on the parity of the request, the reply is sent back sync or async.
handle_request(Request, _From, sync, _Link) ->
	{reply, Request, async};

handle_request(Request, From, async, _Link) ->
	spawn(fun() ->
		ok = iris:reply(From, Request)
	end),
	{noreply, sync}.

%% No state to clean up.
terminate(_Reason, _State) -> ok.


%% =============================================================================
%% Unused Iris server callback methods (shuts the compiler up)
%% =============================================================================

handle_broadcast(_Message, State) ->
	{stop, unimplemented, State}.

handle_publish(_Topic, _Event, State, _Link) ->
	{stop, unimplemented, State}.

handle_tunnel(_Tunnel, State, _Link) ->
	{stop, unimplemented, State}.

handle_drop(_Reason, State) ->
	{stop, unimplemented, State}.
