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
-export([start_link/4, schedule/3, replenish/2]).


%% =============================================================================
%% External API functions
%% =============================================================================

%% Spawns a bounded mailbox forwarder and links it to the current process.
-spec start_link(Owner :: pid(), Type :: atom(), Limit :: pos_integer(),
  Logger :: iris_logger:logger()) -> pid().

start_link(Owner, Type, Limit, Logger) ->
  spawn_link(fun() -> limit(Owner, Type, Limit, 0, 1, Logger) end).


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
%% Internal functions
%% =============================================================================

%% Forwards inbound messages to the owner's message queue, given that the total
%% memory consumption is below a threshold. Discards otherwise.
-spec limit(Owner :: pid(), Type :: atom(), Limit :: pos_integer(), Used :: non_neg_integer(),
  Id :: pos_integer(), Logger :: iris_logger:logger()) -> no_return().

limit(Owner, Type, Limit, Used, Id, Logger) ->
  receive
    {schedule, Data, Message} ->
      iris_logger:debug(Logger, {"scheduling arrived ~p", [Type]}, [{Type, Id}, {data, Data}]),

      % Make sure there is enough memory for the message
      Size = byte_size(Data),
      case Used + Size =< Limit of
        true ->
          Owner ! {Id, Message},
          limit(Owner, Type, Limit, Used + Size, Id + 1, Logger);
        false ->
          % Not enough memory in the message queue
          iris_logger:error(Logger, {"~p exceeded memory allowance", [Type]},
            [{Type, Id}, {limit, Limit}, {used, Used}, {size, Size}]
          ),
          limit(Owner, Type, Limit, Used, Id + 1, Logger)
      end;
    {replenish, Space} ->
      limit(Owner, Type, Limit, Used - Space, Id, Logger)
  end.
