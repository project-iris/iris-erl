%% Iris Erlang binding
%% Copyright (c) 2013 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

-module(connect_tests).
-include_lib("eunit/include/eunit.hrl").
-include("configs.hrl").


%% Tests multiple concurrent client connections.
connect_test() ->
	% Test specific configurations
	ConfClients = 100,

	% Start a batch of parallel connections
	Barrier = iris_barrier:new(ConfClients),
	lists:foreach(fun(_Client) ->
		spawn(fun() ->
			try
				% Connect to the local relay
				{ok, Conn} = iris_client:start(?CONFIG_RELAY),
				iris_barrier:sync(Barrier),

				% Disconnect from the local relay
				ok = iris_client:stop(Conn),
				iris_barrier:exit(Barrier)
			catch
				error:Exception -> iris_barrier:exit(Barrier, Exception)
			end
		end)
	end, lists:seq(1, ConfClients)),

	% Schedule the parallel operations
	ok = iris_barrier:wait(Barrier),
	ok = iris_barrier:wait(Barrier).
