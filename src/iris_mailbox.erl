%% Iris Erlang binding
%% Copyright (c) 2014 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

%% Contains the bounded mailbox implementation for inbound events. In essence
%% it is a fast message forwarder that schedules inbound messages into the
%% owner's mailbox if there is capacity available, or discards them otherwise.

%% @private

-module(iris_mailbox).
-export([start_link/3, schedule/3, replenish/2, limit/4]).


%% =============================================================================
%% External API functions
%% =============================================================================

%% Spawns a bounded mailbox forwarder and links it to the current process.
-spec start_link(Owner :: pid(), Type :: atom(), Limit :: pos_integer()) -> pid().

start_link(Owner, Type, Limit) ->
  spawn_link(?MODULE, limit, [Owner, Type, Limit, 0]).


%% Schedules a message into the remote mailbox if and only if the associated
%% space requirements can be satisfied.
-spec schedule(Limiter :: pid(), Size :: non_neg_integer(), Message :: term()) -> ok.

schedule(Limiter, Size, Message) ->
  Limiter ! {schedule, Size, Message},
  ok.


%% Grants additional space allowance to a bounded mailbox.
-spec replenish(Limiter :: pid(), Space :: non_neg_integer()) -> ok.

replenish(Limiter, Space) ->
  Limiter ! {replenish, Space},
  ok.


%% =============================================================================
%% Internal API functions
%% =============================================================================

%% Forwards inbound messages to the owner's message queue, given that the total
%% memory consumption is below a threshold. Discards otherwise.
-spec limit(Owner :: pid(), Type :: atom(), Limit :: pos_integer(),
  Used :: non_neg_integer()) -> no_return().

limit(Owner, Type, Limit, Used) ->
  receive
    {schedule, Size, Message} when Used + Size =< Limit ->
      Owner ! Message,
      limit(Owner, Type, Limit, Used + Size);
    {schedule, Size, _Message} ->
      io:format("~p exceeded memory allowance: limit=~p used=~p size=~p~n", [Type, Limit, Used, Size]),
      limit(Owner, Type, Limit, Used);
    {replenish, Space} ->
      limit(Owner, Type, Limit, Used - Space)
  end.
