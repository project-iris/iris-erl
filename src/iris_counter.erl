%% Iris Erlang binding
%% Copyright (c) 2014 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

%% Contains a very simple atomic counter to return auto incrementing values.
%% Although it seems to violate all Erlang best practices, it is used solely for
%% logging purposes, so don't fret about it too much.

%% @private

-module(iris_counter).
-export([next_id/1]).


%% Fetches the next auto incrementing unique id for the given counter.
-spec next_id(Name :: atom()) -> pos_integer().

next_id(Name) ->
  % Generate a canonical name for the counter and look it up
  Counter = list_to_atom(lists:flatten(io_lib:format("iris_counter_~p", [Name]))),
  case whereis(Counter) of
    undefined ->
      % The counter does not exist, spawn and register
      Pid = spawn(fun() -> loop(1) end),
      try
        case register(Counter, Pid) of
          true -> ok
        end
      catch
        error:badarg ->
            % Race, already registered, kill the new
          Pid ! exit
      end,

      % Retry fetching an id
      next_id(Name);
    Pid ->
      Pid ! {self(), next},
      receive
        {id, Id} -> Id
      end
  end.


%% Loops around, waiting for auto id requests.
-spec loop(Id :: pos_integer()) -> ok.

loop(Id) ->
  receive
    {From, next} ->
      From ! {id, Id},
      loop(Id + 1);
    exit ->
      ok
  end.
