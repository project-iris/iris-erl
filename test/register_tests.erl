%% Iris Erlang binding
%% Copyright (c) 2014 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

-module(register_tests).
-include_lib("eunit/include/eunit.hrl").
-include("configs.hrl").

-behaviour(iris_server).
-export([init/2, handle_broadcast/2, handle_request/3, handle_tunnel/3,
	handle_drop/2, terminate/2]).


%% Tests multiple concurrent service registrations.
register_test() ->
	% Test specific configurations
	ConfServices = 100,

	% Start a batch of parallel connections
	Barrier = iris_barrier:new(ConfServices),
	lists:foreach(fun(_Service) ->
		spawn(fun() ->
			try
				% Register a new service to the relay
				{ok, Server} = iris_server:start_link(?CONFIG_RELAY, ?CONFIG_CLUSTER, ?MODULE, nil),
				iris_barrier:sync(Barrier),

				% Unregister the service
				ok = iris_server:stop(Server),
				iris_barrier:exit(Barrier)
			catch
				Exception -> iris_barrier:exit(Exception), ok
			end
		end)
	end, lists:seq(1, ConfServices)),

	% Schedule the parallel operations
	ok = iris_barrier:wait(Barrier),
	ok = iris_barrier:wait(Barrier).


%% =============================================================================
%% Iris server callback methods
%% =============================================================================

%% Notifies the tester of the successful init call.
init(_Conn, nil) -> {ok, no_state}.

%% Notifies the tester of the successful terminate call.
terminate(_Reason, _State) ->	ok.


%% =============================================================================
%% Unused Iris server callback methods (shuts the compiler up)
%% =============================================================================

handle_broadcast(_Message, State) ->
	{stop, unimplemented, State}.

handle_request(_Request, _From, State) ->
	{stop, unimplemented, State}.

handle_tunnel(_Tunnel, State, _Link) ->
	{stop, unimplemented, State}.

handle_drop(_Reason, State) ->
	{stop, unimplemented, State}.
