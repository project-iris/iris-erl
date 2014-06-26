%% Iris Erlang binding
%% Copyright (c) 2014 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

%% Contains an thread barrier implementation, where a batch of threads can sync
%% multiple phases of execution, with the added possibility of leaving early on
%% and even reporting occurred errors.

%% private

-module(iris_barrier).
-export([new/1, sync/1, exit/1, exit/2, wait/1, barrier/5]).


%% =============================================================================
%% External API functions
%% =============================================================================

%% Creates a new barrier with a capacity of the given amount of threads.
-spec new(Threads :: pos_integer()) -> Barrier :: pid().

new(Threads) ->
	spawn(?MODULE, barrier, [Threads, [], [], [], nil]).


%% Synchronizes a worker thread with others for the next phase.
-spec sync(Barrier :: pid()) -> ok.

sync(Barrier) ->
	Barrier ! {sync, self()},
	receive
		{Barrier, ok} -> ok
	end.


%% Cleanly exists the barrier, synchronizing it with the phase end.
-spec exit(Barrier :: pid()) -> ok.

exit(Barrier) ->
	Barrier ! {exit, self()},
	receive
		{Barrier, ok} -> ok
	end.


%% Exists the barrier with a failure, synchronizing it with the phase end.
-spec exit(Barrier :: pid(), Error :: term()) -> ok.

exit(Barrier, Error) ->
	Barrier ! {exit, Error, self()},
	receive
		{Barrier, ok} -> ok
	end.


%% Waits until all workers reach the barrier, gathers any errors occurred and
%% permits all waiting to continue execution.
-spec wait(Barrier :: pid()) -> ok.

wait(Barrier) ->
	Barrier ! {wait, self()},
	receive
		{Barrier, ok}            -> ok;
		{Barrier, error, Errors} -> {error, Errors}
	end.


%% =============================================================================
%% Internal API functions
%% =============================================================================

%% Barrier exhausted, everybody quit, terminate.
barrier(0, [], [], [], nil) -> ok;

%% All running threads synced up, owner is waiting, begin next phase.
barrier(0, Waiting, Exiting, Errors, Owner) when Owner =/= nil ->
	% Signal all threads waiting and exiting at the barrier
	lists:foreach(fun(Pid) -> Pid ! {self(), ok} end, Waiting),
	lists:foreach(fun(Pid) ->	Pid ! {self(), ok} end, Exiting),

	% Signal owner of any occurred errors
	case Errors of
		[] -> Owner ! {self(), ok};
		_  -> Owner ! {self(), error, Errors}
	end,

	% Start looping if there's anyone left
	barrier(length(Waiting), [], [], [], nil);

%% Normal operation, either wait for a sync, exit or wait request.
barrier(Running, Waiting, Exiting, Errors, Owner) ->
	receive
		{sync, Pid}        -> barrier(Running-1, [Pid | Waiting], Exiting, Errors, Owner);
		{exit, Pid}        -> barrier(Running-1, Waiting, [Pid | Exiting], Errors, Owner);
		{exit, Error, Pid} -> barrier(Running-1, Waiting, [Pid | Exiting], [Error | Errors], Owner);
		{wait, Pid}        -> barrier(Running, Waiting, Exiting, Errors, Pid)
	end.
