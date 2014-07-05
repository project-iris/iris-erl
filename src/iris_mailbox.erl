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
-export([start_link/3, schedule/2, replenish/2]).


%% =============================================================================
%% External API functions
%% =============================================================================

%% Spawns a bounded mailbox forwarder and links it to the current process.
-spec start_link(Owner :: pid(), Limit :: pos_integer(),
  Logger :: iris_logger:logger()) -> pid().

start_link(Owner, Limit, Logger) ->
  spawn_link(fun() -> limit(Owner, Limit, 0, 1, Logger) end).


%% Schedules a message into the remote mailbox if and only if the associated
%% space requirements can be satisfied.
-spec schedule(Limiter :: pid(), Message :: term()) -> ok.

schedule(Limiter, Message) ->
  Limiter ! {schedule, Message},
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
-spec limit(Owner :: pid(), Limit :: pos_integer(), Used :: non_neg_integer(),
  Id :: pos_integer(), Logger :: iris_logger:logger()) -> no_return().

limit(Owner, Limit, Used, AutoId, Logger) ->
  receive
    {schedule, Message} ->
      % Log the arrival of a message and calculate the binary size
      % Yep, matching against the message internals is extremely ugly, but I see
      % no better alternative apart from duplicating the whole limiter logic.
      {Size, Context} = case Message of
        {handle_broadcast, Payload} ->
          iris_logger:debug(Logger, "scheduling arrived broadcast", [{broadcast, AutoId}, {data, Payload}]),
          {byte_size(Payload), {broadcast, AutoId}};
        {handle_request, Id, Request, Timeout, _Expiry} ->
          iris_logger:debug(Logger, "scheduling arrived request", [{remote_request, Id}, {data, Request}, {timeout, Timeout}]),
          {byte_size(Request), {remote_request, Id}};
        {handle_event, Event} ->
          iris_logger:debug(Logger, "scheduling arrived event", [{event, AutoId}, {data, Event}]),
          {byte_size(Event), {event, AutoId}}
      end,

      % Make sure there is enough memory for the message
      case Used + Size =< Limit of
        true ->
          Owner ! {Context, Message, self()},
          limit(Owner, Limit, Used + Size, AutoId + 1, Logger);
        false ->
          % Not enough memory in the message queue
          iris_logger:error(Logger, "queue exceeded memory allowance",
            [Context, {limit, Limit}, {used, Used}, {size, Size}]
          ),
          limit(Owner, Limit, Used, AutoId + 1, Logger)
      end;
    {replenish, Space} ->
      limit(Owner, Limit, Used - Space, AutoId, Logger)
  end.
