%% Iris Erlang binding
%% Copyright (c) 2014 Project Iris. All rights reserved.
%%
%% The current language binding is an official support library of the Iris
%% cloud messaging framework, and as such, the same licensing terms apply.
%% For details please see http://iris.karalabe.com/downloads#License

%% Contains the bounded mailbox implementation for inbound events. In essence
%% it is a fast message forwarder that schedules inbound messages into the
%% owner's mailbox if there is capacity available, or discards them otherwise.

-module(iris_mailbox).
-export([limit/4]).

%% Forwards inbound messages to the owner's message queue, given that the total
%% memory consumption is below a threshold. Discards otherwise.
-spec limit(Owner :: pid(), Type :: [byte()], Limit :: pos_integer(),
  Used :: non_neg_integer()) -> no_return().

limit(Owner, Type, Limit, Used) ->
  receive
    {schedule, Size, Message} when Used + Size =< Limit ->
      Owner ! Message,
      limit(Owner, Type, Limit, Used + Size);
    {schedule, Size, _Message} ->
      io:format("~s exceeded memory allowance: limit=~p used=~p size=~p", [Type, Limit, Used, Size]),
      limit(Owner, Type, Limit, Used);
    {grant, Space} ->
      limit(Owner, Type, Limit, Used - Space)
  end.
