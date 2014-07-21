%% Iris Erlang binding
%% Copyright (c) 2014 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

-module(benchmark).
-export([iterated/3, threaded/5]).


% Runs an iterated benchmark, each function call finishing before the next.
iterated(Name, Iters, Func) ->
	io:format(user, "~s   ", [atom_to_list(Name)]),

	Start = erlang:now(),
	ok    = run(Iters, Func),
	Diff  = timer:now_diff(erlang:now(), Start),

	io:format(user, "~B ops   ~p ns/op~n", [Iters, Diff * 1000 / Iters]).


% Runs a concurrent benchmark, with parallel worker threads and a single
% result collector.
threaded(Name, Threads, Iters, Map, Reduce) ->
	io:format(user, "~s_~B_threads   ", [atom_to_list(Name), Threads]),
	Start = erlang:now(),

	% Spawn the mapping threads
	lists:foreach(fun(Index) ->
		% Calculate the iterations alloted to this thread
		Batch = Iters / Threads,
		Count = case Index of
			Threads -> Iters - trunc((Index - 1) * Batch);
			_       -> trunc(Index * Batch) - trunc((Index - 1) * Batch)
		end,

		spawn(fun() -> run(Count, Map) end)
	end, lists:seq(1, Threads)),

	% Run the reducer and wait for it
	ok   = run(Iters, Reduce),
	Diff = timer:now_diff(erlang:now(), Start),

	io:format(user, "~B ops   ~p ns/op~n", [Iters, Diff * 1000 / Iters]).


% Simple iterator to repeat a function call N times.
run(0, _Func)   -> ok;
run(Left, Func) ->
	ok = Func(),
	run(Left-1, Func).
